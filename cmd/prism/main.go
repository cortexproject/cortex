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
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/web/api/v1"
	"github.com/weaveworks/scope/common/middleware"
	"golang.org/x/net/context"

	"github.com/weaveworks/prism"
	"github.com/weaveworks/prism/chunk"
	"github.com/weaveworks/prism/ingester"
	"github.com/weaveworks/prism/ring"
	"github.com/weaveworks/prism/ui"
	"github.com/weaveworks/prism/user"
)

const (
	modeDistributor = "distributor"
	modeIngester    = "ingester"

	infName          = "eth0"
	userIDHeaderName = "X-Scope-OrgID"
)

var (
	requestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "prism",
		Name:      "request_duration_seconds",
		Help:      "Time (in seconds) spent serving HTTP requests.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "route", "status_code", "ws"})
	instr = middleware.Merge(
		middleware.Logging,
		middleware.Instrument{
			Duration: requestDuration,
		},
	).Wrap
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

	distributorConfig prism.DistributorConfig
}

func main() {
	var cfg cfg
	flag.StringVar(&cfg.mode, "mode", modeDistributor, "Mode (distributor, ingester).")
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
	flag.IntVar(&cfg.distributorConfig.ReadReplicas, "distributor.read-replicas", 3, "The number of available ingesters to read from.")
	flag.IntVar(&cfg.distributorConfig.WriteReplicas, "distributor.write-replicas", 3, "The number of available ingesters to write to.")
	flag.IntVar(&cfg.distributorConfig.MinReadSuccesses, "distributor.min-read-successes", 2, "The minimum number of ingesters from which a read must succeed.")
	flag.IntVar(&cfg.distributorConfig.MinWriteSuccesses, "distributor.min-write-successes", 2, "The minimum number of ingesters to which a write must succeed.")
	flag.DurationVar(&cfg.distributorConfig.HeartbeatTimeout, "distributor.heartbeat-timeout", time.Minute, "The heartbeat timeout after which ingesters are skipped for reads/writes.")
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
	case modeDistributor:
		ring := ring.New(consul)
		cfg.distributorConfig.Ring = ring
		cfg.distributorConfig.ClientFactory = func(address string) (*prism.IngesterClient, error) {
			return prism.NewIngesterClient(address, cfg.remoteTimeout)
		}
		defer ring.Stop()
		setupDistributor(cfg.distributorConfig, chunkStore)
		if err != nil {
			log.Fatalf("Error initializing distributor: %v", err)
		}
	case modeIngester:
		registration, err := ring.RegisterIngester(consul, cfg.listenPort, cfg.numTokens)
		prometheus.MustRegister(registration)
		if err != nil {
			// This only happens for errors in configuration & set-up, not for
			// network errors.
			log.Fatalf("Could not register ingester: %v", err)
		}
		defer registration.Unregister()
		cfg := ingester.Config{
			FlushCheckPeriod: cfg.flushPeriod,
			MaxChunkAge:      cfg.maxChunkAge,
		}
		ing := setupIngester(chunkStore, cfg)
		defer ing.Stop()
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

func setupChunkStore(cfg cfg) (chunk.Store, error) {
	var chunkCache *chunk.Cache
	if cfg.memcachedHostname != "" {
		chunkCache = &chunk.Cache{
			Memcache: chunk.NewMemcacheClient(chunk.MemcacheConfig{
				Host:           cfg.memcachedHostname,
				Service:        cfg.memcachedService,
				Timeout:        cfg.memcachedTimeout,
				UpdateInterval: 1 * time.Minute,
			}),
			Expiration: cfg.memcachedExpiration,
		}
	}
	chunkStore, err := chunk.NewAWSStore(chunk.StoreConfig{
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
	cfg prism.DistributorConfig,
	chunkStore chunk.Store,
) {
	distributor := prism.NewDistributor(cfg)
	prometheus.MustRegister(distributor)

	prefix := "/api/prom"
	http.Handle(prefix+"/push", instr(prism.AppenderHandler(distributor)))

	// TODO: Move querier to separate binary.
	setupQuerier(distributor, chunkStore, prefix)
}

// setupQuerier sets up a complete querying pipeline:
//
// PromQL -> MergeQuerier -> Distributor -> IngesterQuerier -> Ingester
//              |
//              `----------> ChunkQuerier -> DynamoDB/S3
func setupQuerier(
	distributor *prism.Distributor,
	chunkStore chunk.Store,
	prefix string,
) {
	querier := prism.MergeQuerier{
		Queriers: []prism.Querier{
			distributor,
			&prism.ChunkQuerier{
				Store: chunkStore,
			},
		},
	}
	engine := promql.NewEngine(querier, nil)

	api := v1.NewAPI(engine, querier)
	router := route.New(func(r *http.Request) (context.Context, error) {
		userID := r.Header.Get(userIDHeaderName)
		if r.Method != "OPTIONS" && userID == "" {
			// For now, getting the user ID from basic auth allows for easy testing
			// with Grafana.
			// TODO: Remove basic auth support.
			userID, _, _ = r.BasicAuth()
			if userID == "" {
				return nil, fmt.Errorf("missing user ID")
			}
		}
		return user.WithID(context.Background(), userID), nil
	})
	api.Register(router.WithPrefix(prefix + "/api/v1"))
	http.Handle("/", router)

	http.Handle(prefix+"/graph", instr(ui.GraphHandler()))
	http.Handle(prefix+"/static/", instr(ui.StaticAssetsHandler(prefix+"/static/")))
}

func setupIngester(
	chunkStore chunk.Store,
	cfg ingester.Config,
) *ingester.Ingester {
	ingester, err := ingester.New(cfg, chunkStore)
	if err != nil {
		log.Fatal(err)
	}
	prometheus.MustRegister(ingester)

	http.Handle("/push", instr(prism.AppenderHandler(ingester)))
	http.Handle("/query", instr(prism.QueryHandler(ingester)))
	http.Handle("/label_values", instr(prism.LabelValuesHandler(ingester)))
	return ingester
}
