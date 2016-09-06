// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/storage/local"
	"github.com/weaveworks/scope/common/middleware"
	"golang.org/x/net/context"

	"github.com/tomwilkie/frankenstein"
	"github.com/tomwilkie/frankenstein/api"
	"github.com/tomwilkie/frankenstein/ring"
)

const (
	distributor = "distributor"
	ingester    = "ingester"
	infName     = "eth0"
)

var (
	requestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "frankenstein",
		Name:      "request_duration_seconds",
		Help:      "Time (in seconds) spent serving HTTP requests.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "route", "status_code", "ws"})
)

func init() {
	prometheus.MustRegister(requestDuration)
}

type cfg struct {
	mode                 string
	listenPort           int
	consulHost           string
	consulPrefix         string
	s3URL                string
	dynamodbURL          string
	dynamodbCreateTables bool
	memcachedHostname    string
	memcachedTimeout     time.Duration
	memcachedExpiration  time.Duration
	memcachedService     string
	remoteTimeout        time.Duration
	flushPeriod          time.Duration
	maxChunkAge          time.Duration
	numTokens            int
}

func main() {
	var cfg cfg
	flag.StringVar(&cfg.mode, "mode", distributor, "Mode (distributor, ingester).")
	flag.IntVar(&cfg.listenPort, "web.listen-port", 9094, "HTTP server listen port.")
	flag.StringVar(&cfg.consulHost, "consul.hostname", "localhost:8500", "Hostname and port of Consul.")
	flag.StringVar(&cfg.consulPrefix, "consul.prefix", "collectors/", "Prefix for keys in Consul.")
	flag.StringVar(&cfg.s3URL, "s3.url", "localhost:4569", "S3 endpoint URL.")
	flag.StringVar(&cfg.dynamodbURL, "dynamodb.url", "localhost:8000", "DynamoDB endpoint URL.")
	flag.BoolVar(&cfg.dynamodbCreateTables, "dynamodb.create-tables", false, "Create required DynamoDB tables on startup.")
	flag.StringVar(&cfg.memcachedHostname, "memcached.hostname", "", "Hostname for memcached service to use when caching chunks. If empty, no memcached will be used.")
	flag.DurationVar(&cfg.memcachedTimeout, "memcached.timeout", 100*time.Millisecond, "Maximum time to wait before giving up on memcached requests.")
	flag.DurationVar(&cfg.memcachedExpiration, "memcached.expiration", 0, "How long chunks stay in the memcache.")
	flag.StringVar(&cfg.memcachedService, "memcached.service", "memcached", "SRV service used to discover memcache servers.")
	flag.DurationVar(&cfg.remoteTimeout, "remote.timeout", 5*time.Second, "Timeout for downstream ingesters.")
	flag.DurationVar(&cfg.flushPeriod, "ingester.flush-period", 1*time.Minute, "Period with which to attempt to flush chunks.")
	flag.DurationVar(&cfg.maxChunkAge, "ingester.max-chunk-age", 10*time.Minute, "Maximum chunk age before flushing.")
	flag.IntVar(&cfg.numTokens, "ingester.num-tokens", 128, "Number of tokens for each ingester.")
	flag.Parse()

	chunkStore, err := setupChunkStore(cfg)
	if err != nil {
		log.Fatalf("Error initializing chunk store: %v", err)
	}

	consul, err := ring.NewConsulClient(cfg.consulHost)
	if err != nil {
		log.Fatalf("Error initializing Consul client: %v", err)
	}
	consul = ring.PrefixClient(consul, cfg.consulPrefix)

	switch cfg.mode {
	case distributor:
		ring := ring.NewRing(consul)
		defer ring.Stop()
		setupDistributor(cfg, ring, chunkStore)
		if err != nil {
			log.Fatalf("Error initializing distributor: %v", err)
		}
	case ingester:
		registration, err := ring.RegisterIngester(consul, cfg.listenPort, cfg.numTokens)
		if err != nil {
			// This only happens for errors in configuration & set-up, not for
			// network errors.
			log.Fatalf("Could not register ingester: %v", err)
		}
		defer registration.Unregister()
		cfg := local.IngesterConfig{
			FlushCheckPeriod: cfg.flushPeriod,
			MaxChunkAge:      cfg.maxChunkAge,
		}
		ingester := setupIngester(chunkStore, cfg)
		defer ingester.Stop()
	default:
		log.Fatalf("Mode %s not supported!", cfg.mode)
	}

	http.Handle("/metrics", prometheus.Handler())
	go http.ListenAndServe(fmt.Sprintf(":%d", cfg.listenPort), nil)

	term := make(chan os.Signal)
	signal.Notify(term, os.Interrupt, syscall.SIGTERM)
	<-term
	log.Warn("Received SIGTERM, exiting gracefully...")
}

func setupChunkStore(cfg cfg) (frankenstein.ChunkStore, error) {
	var chunkCache *frankenstein.ChunkCache
	if cfg.memcachedHostname != "" {
		chunkCache = &frankenstein.ChunkCache{
			Memcache: frankenstein.NewMemcacheClient(frankenstein.MemcacheConfig{
				Host:           cfg.memcachedHostname,
				Service:        cfg.memcachedService,
				Timeout:        cfg.memcachedTimeout,
				UpdateInterval: 1 * time.Minute,
			}),
			Expiration: cfg.memcachedExpiration,
		}
	}
	chunkStore, err := frankenstein.NewAWSChunkStore(frankenstein.ChunkStoreConfig{
		S3URL:       cfg.s3URL,
		DynamoDBURL: cfg.dynamodbURL,
		ChunkCache:  chunkCache,
	})
	if err != nil {
		return nil, err
	}
	if cfg.dynamodbCreateTables {
		if err = chunkStore.CreateTables(); err != nil {
			return nil, err
		}
	}
	return chunkStore, err
}

func setupDistributor(
	cfg cfg,
	ring *ring.Ring,
	chunkStore frankenstein.ChunkStore,
) {
	clientFactory := func(hostname string) (*frankenstein.IngesterClient, error) {
		return frankenstein.NewIngesterClient(hostname, cfg.remoteTimeout), nil
	}
	distributor := frankenstein.NewDistributor(frankenstein.DistributorConfig{
		Ring:          ring,
		ClientFactory: clientFactory,
	})
	prometheus.MustRegister(distributor)

	prefix := "/api/prom"
	http.Handle(prefix+"/push", frankenstein.AppenderHandler(distributor))

	// TODO: Move querier to separate binary.
	setupQuerier(distributor, chunkStore, prefix)
}

// setupQuerier sets up a complete querying pipeline:
//
// PromQL -> MergeQuerier -> Distributor -> IngesterQuerier -> Ingester
//              |
//              +----------> ChunkQuerier -> DynamoDB/S3
func setupQuerier(
	distributor *frankenstein.Distributor,
	chunkStore frankenstein.ChunkStore,
	prefix string,
) {
	newQuerier := func(ctx context.Context) local.Querier {
		return frankenstein.MergeQuerier{
			Queriers: []frankenstein.Querier{
				distributor,
				&frankenstein.ChunkQuerier{
					Store: chunkStore,
				},
			},
			Context: ctx,
		}
	}

	api := api.New(newQuerier)
	router := route.New()
	api.Register(router.WithPrefix(prefix + "/api/v1"))
	http.Handle("/", router)

	http.Handle(prefix+"/graph", frankenstein.GraphHandler())
	http.Handle(prefix+"/static/", frankenstein.StaticAssetsHandler(prefix+"/static/"))
}

func setupIngester(
	chunkStore frankenstein.ChunkStore,
	cfg local.IngesterConfig,
) *local.Ingester {
	ingester, err := local.NewIngester(cfg, chunkStore)
	if err != nil {
		log.Fatal(err)
	}
	prometheus.MustRegister(ingester)

	instr := middleware.Merge(
		middleware.Logging,
		middleware.Instrument{
			Duration: requestDuration,
		},
	).Wrap

	http.Handle("/push", instr(frankenstein.AppenderHandler(ingester)))
	http.Handle("/query", instr(frankenstein.QueryHandler(ingester)))
	http.Handle("/label_values", instr(frankenstein.LabelValuesHandler(ingester)))
	return ingester
}
