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

	"github.com/weaveworks/cortex"
	"github.com/weaveworks/cortex/chunk"
	"github.com/weaveworks/cortex/ingester"
	"github.com/weaveworks/cortex/querier"
	"github.com/weaveworks/cortex/ring"
	"github.com/weaveworks/cortex/ui"
	"github.com/weaveworks/cortex/user"
)

const (
	modeDistributor = "distributor"
	modeIngester    = "ingester"

	infName          = "eth0"
	userIDHeaderName = "X-Scope-OrgID"
)

var (
	requestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "request_duration_seconds",
		Help:      "Time (in seconds) spent serving HTTP requests.",

		// Cortex latency can be very low (ingesters typically take a few ms to
		// process a request), all the way to very high (queries can take tens of
		// second).  As its important, we use 10 buckets.  Smallest is 128us
		// biggest is 130s.
		Buckets: prometheus.ExponentialBuckets(0.000128, 4, 10),
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
	numTokens            int
	logSuccess           bool

	ingesterConfig    ingester.Config
	distributorConfig cortex.DistributorConfig
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
	flag.DurationVar(&cfg.ingesterConfig.FlushCheckPeriod, "ingester.flush-period", 1*time.Minute, "Period with which to attempt to flush chunks.")
	flag.DurationVar(&cfg.ingesterConfig.RateUpdatePeriod, "ingester.rate-update-period", 15*time.Second, "Period with which to update the per-user ingestion rates.")
	flag.DurationVar(&cfg.ingesterConfig.MaxChunkAge, "ingester.max-chunk-age", 10*time.Minute, "Maximum chunk age before flushing.")
	flag.IntVar(&cfg.numTokens, "ingester.num-tokens", 128, "Number of tokens for each ingester.")
	flag.IntVar(&cfg.distributorConfig.ReplicationFactor, "distributor.replication-factor", 3, "The number of ingesters to write to and read from.")
	flag.IntVar(&cfg.distributorConfig.MinReadSuccesses, "distributor.min-read-successes", 2, "The minimum number of ingesters from which a read must succeed.")
	flag.DurationVar(&cfg.distributorConfig.HeartbeatTimeout, "distributor.heartbeat-timeout", time.Minute, "The heartbeat timeout after which ingesters are skipped for reads/writes.")
	flag.BoolVar(&cfg.logSuccess, "log.success", false, "Log successful requests")
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
		ring := ring.New(consul, cfg.distributorConfig.HeartbeatTimeout)
		cfg.distributorConfig.Ring = ring
		cfg.distributorConfig.ClientFactory = func(address string) (*cortex.IngesterClient, error) {
			return cortex.NewIngesterClient(address, cfg.remoteTimeout)
		}
		defer ring.Stop()
		setupDistributor(cfg.distributorConfig, chunkStore, cfg.logSuccess)
	case modeIngester:
		registration, err := ring.RegisterIngester(consul, cfg.listenPort, cfg.numTokens)
		if err != nil {
			// This only happens for errors in configuration & set-up, not for
			// network errors.
			log.Fatalf("Could not register ingester: %v", err)
		}
		ing := setupIngester(chunkStore, cfg.ingesterConfig, cfg.logSuccess)

		// Deferring a func to make ordering obvious
		defer func() {
			registration.ChangeState(ring.Leaving)
			ing.Stop()
			registration.Unregister()
		}()

		prometheus.MustRegister(registration)
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
	cfg cortex.DistributorConfig,
	chunkStore chunk.Store,
	logSuccess bool,
) {
	distributor, err := cortex.NewDistributor(cfg)
	if err != nil {
		log.Fatal(err)
	}
	prometheus.MustRegister(distributor)

	prefix := "/api/prom"
	http.Handle(prefix+"/push", instrument(logSuccess, cortex.AppenderHandler(distributor, handleDistributorError)))

	// TODO: Move querier to separate binary.
	setupQuerier(distributor, chunkStore, prefix, logSuccess)
}

func handleDistributorError(w http.ResponseWriter, err error) {
	switch e := err.(type) {
	case cortex.IngesterError:
		switch {
		case 400 <= e.StatusCode && e.StatusCode < 500:
			log.Warnf("append err: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	log.Errorf("append err: %v", err)
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

// setupQuerier sets up a complete querying pipeline:
//
// PromQL -> MergeQuerier -> Distributor -> IngesterQuerier -> Ingester
//              |
//              `----------> ChunkQuerier -> DynamoDB/S3
func setupQuerier(
	distributor *cortex.Distributor,
	chunkStore chunk.Store,
	prefix string,
	logSuccess bool,
) {
	querier := querier.MergeQuerier{
		Queriers: []querier.Querier{
			distributor,
			&querier.ChunkQuerier{
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

	http.Handle(prefix+"/user_stats", instrument(logSuccess, cortex.DistributorUserStatsHandler(distributor.UserStats)))

	http.Handle(prefix+"/graph", instrument(logSuccess, ui.GraphHandler()))
	http.Handle(prefix+"/static/", instrument(logSuccess, ui.StaticAssetsHandler(prefix+"/static/")))
}

func setupIngester(
	chunkStore chunk.Store,
	cfg ingester.Config,
	logSuccess bool,
) *ingester.Ingester {
	ingester, err := ingester.New(cfg, chunkStore)
	if err != nil {
		log.Fatal(err)
	}
	prometheus.MustRegister(ingester)

	http.Handle("/push", instrument(logSuccess, cortex.AppenderHandler(ingester, handleIngesterError)))
	http.Handle("/query", instrument(logSuccess, cortex.QueryHandler(ingester)))
	http.Handle("/label_values", instrument(logSuccess, cortex.LabelValuesHandler(ingester)))
	http.Handle("/user_stats", instrument(logSuccess, cortex.IngesterUserStatsHandler(ingester.UserStats)))
	return ingester
}

func handleIngesterError(w http.ResponseWriter, err error) {
	switch err {
	case ingester.ErrOutOfOrderSample, ingester.ErrDuplicateSampleForTimestamp:
		log.Warnf("append err: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
	default:
		log.Errorf("append err: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// instrument instruments a handler.
func instrument(logSuccess bool, handler http.Handler) http.Handler {
	return middleware.Merge(
		middleware.Log{
			LogSuccess: logSuccess,
		},
		middleware.Instrument{
			Duration: requestDuration,
		},
	).Wrap(handler)
}
