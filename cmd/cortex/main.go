package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/mwitkow/go-grpc-middleware"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	"github.com/opentracing/opentracing-go"
	"github.com/weaveworks/scope/common/middleware"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/web/api/v1"

	"github.com/weaveworks/cortex"
	"github.com/weaveworks/cortex/chunk"
	"github.com/weaveworks/cortex/distributor"
	"github.com/weaveworks/cortex/ingester"
	"github.com/weaveworks/cortex/querier"
	"github.com/weaveworks/cortex/ring"
	"github.com/weaveworks/cortex/ruler"
	"github.com/weaveworks/cortex/ui"
	"github.com/weaveworks/cortex/user"
	cortex_grpc_middleware "github.com/weaveworks/cortex/util/middleware"
)

const (
	modeDistributor = "distributor"
	modeIngester    = "ingester"
	modeRuler       = "ruler"

	infName = "eth0"
)

var (
	requestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "request_duration_seconds",
		Help:      "Time (in seconds) spent serving HTTP requests.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "route", "status_code", "ws"})
)

func init() {
	prometheus.MustRegister(requestDuration)
}

type cfg struct {
	mode                     string
	listenPort               int
	consulHost               string
	consulPrefix             string
	s3URL                    string
	dynamodbURL              string
	dynamodbCreateTables     bool
	dynamodbPollInterval     time.Duration
	dynamodbDailyBucketsFrom string
	memcachedHostname        string
	memcachedTimeout         time.Duration
	memcachedExpiration      time.Duration
	memcachedService         string
	remoteTimeout            time.Duration
	numTokens                int
	logSuccess               bool
	watchDynamo              bool

	ingesterConfig    ingester.Config
	distributorConfig distributor.Config
	rulerConfig       ruler.Config
}

func main() {
	var cfg cfg
	flag.StringVar(&cfg.mode, "mode", modeDistributor, "Mode (distributor, ingester, ruler).")
	flag.IntVar(&cfg.listenPort, "web.listen-port", 9094, "HTTP server listen port.")
	flag.BoolVar(&cfg.logSuccess, "log.success", false, "Log successful requests")

	flag.StringVar(&cfg.consulHost, "consul.hostname", "localhost:8500", "Hostname and port of Consul.")
	flag.StringVar(&cfg.consulPrefix, "consul.prefix", "collectors/", "Prefix for keys in Consul.")

	flag.StringVar(&cfg.s3URL, "s3.url", "localhost:4569", "S3 endpoint URL.")
	flag.StringVar(&cfg.dynamodbURL, "dynamodb.url", "localhost:8000", "DynamoDB endpoint URL.")
	flag.DurationVar(&cfg.dynamodbPollInterval, "dynamodb.poll-interval", 2*time.Minute, "How frequently to poll DynamoDB to learn our capacity.")
	flag.BoolVar(&cfg.dynamodbCreateTables, "dynamodb.create-tables", false, "Create required DynamoDB tables on startup.")
	flag.StringVar(&cfg.dynamodbDailyBucketsFrom, "dynamodb.daily-buckets-from", "9999-01-01", "The date in the format YYYY-MM-DD of the first day for which DynamoDB index buckets should be day-sized vs. hour-sized.")
	flag.BoolVar(&cfg.watchDynamo, "watch-dynamo", false, "Periodically collect DynamoDB provisioned throughput.")

	flag.StringVar(&cfg.memcachedHostname, "memcached.hostname", "", "Hostname for memcached service to use when caching chunks. If empty, no memcached will be used.")
	flag.StringVar(&cfg.memcachedService, "memcached.service", "memcached", "SRV service used to discover memcache servers.")
	flag.DurationVar(&cfg.memcachedTimeout, "memcached.timeout", 100*time.Millisecond, "Maximum time to wait before giving up on memcached requests.")
	flag.DurationVar(&cfg.memcachedExpiration, "memcached.expiration", 0, "How long chunks stay in the memcache.")

	flag.DurationVar(&cfg.ingesterConfig.FlushCheckPeriod, "ingester.flush-period", 1*time.Minute, "Period with which to attempt to flush chunks.")
	flag.DurationVar(&cfg.ingesterConfig.RateUpdatePeriod, "ingester.rate-update-period", 15*time.Second, "Period with which to update the per-user ingestion rates.")
	flag.DurationVar(&cfg.ingesterConfig.MaxChunkIdle, "ingester.max-chunk-idle", 1*time.Hour, "Maximum chunk idle time before flushing.")
	flag.DurationVar(&cfg.ingesterConfig.MaxChunkAge, "ingester.max-chunk-age", 12*time.Hour, "Maximum chunk age time before flushing.")
	flag.IntVar(&cfg.ingesterConfig.ConcurrentFlushes, "ingester.concurrent-flushes", 25, "Number of concurrent goroutines flushing to dynamodb.")
	flag.IntVar(&cfg.numTokens, "ingester.num-tokens", 128, "Number of tokens for each ingester.")
	flag.IntVar(&cfg.ingesterConfig.GRPCListenPort, "ingester.grpc.listen-port", 9095, "gRPC server listen port.")

	flag.IntVar(&cfg.distributorConfig.ReplicationFactor, "distributor.replication-factor", 3, "The number of ingesters to write to and read from.")
	flag.IntVar(&cfg.distributorConfig.MinReadSuccesses, "distributor.min-read-successes", 2, "The minimum number of ingesters from which a read must succeed.")
	flag.DurationVar(&cfg.distributorConfig.HeartbeatTimeout, "distributor.heartbeat-timeout", time.Minute, "The heartbeat timeout after which ingesters are skipped for reads/writes.")
	flag.DurationVar(&cfg.distributorConfig.RemoteTimeout, "distributor.remote-timeout", 5*time.Second, "Timeout for downstream ingesters.")

	flag.StringVar(&cfg.rulerConfig.ConfigsAPIURL, "ruler.configs.url", "", "URL of configs API server.")
	flag.StringVar(&cfg.rulerConfig.UserID, "ruler.userID", "", "Weave Cloud org to run rules for")
	flag.DurationVar(&cfg.rulerConfig.EvaluationInterval, "ruler.evaluation-interval", 15*time.Second, "How frequently to evaluate rules")

	flag.Parse()

	chunkStore, err := setupChunkStore(cfg)
	if err != nil {
		log.Fatalf("Error initializing chunk store: %v", err)
	}
	if cfg.dynamodbPollInterval < 1*time.Minute {
		log.Warnf("Polling DynamoDB more than once a minute. Likely to get throttled: %v", cfg.dynamodbPollInterval)
	}
	defer chunkStore.Stop()

	if cfg.watchDynamo {
		resourceWatcher, err := chunk.WatchDynamo(cfg.dynamodbURL, cfg.dynamodbPollInterval)
		if err != nil {
			log.Fatalf("Error initializing DynamoDB watcher: %v", err)
		}
		defer resourceWatcher.Stop()
		prometheus.MustRegister(resourceWatcher)
	}

	consul, err := ring.NewConsulClient(cfg.consulHost)
	if err != nil {
		log.Fatalf("Error initializing Consul client: %v", err)
	}
	consul = ring.PrefixClient(consul, cfg.consulPrefix)
	r := ring.New(consul, cfg.distributorConfig.HeartbeatTimeout)
	defer r.Stop()

	router := mux.NewRouter()
	router.Handle("/ring", r)

	switch cfg.mode {
	case modeDistributor:
		cfg.distributorConfig.Ring = r
		setupDistributor(cfg.distributorConfig, chunkStore, router.PathPrefix("/api/prom").Subrouter())

	case modeIngester:
		cfg.ingesterConfig.Ring = r
		registration, err := ring.RegisterIngester(consul, ring.IngesterRegistrationConfig{
			ListenPort: cfg.listenPort,
			GRPCPort:   cfg.ingesterConfig.GRPCListenPort,
			NumTokens:  cfg.numTokens,
		})
		if err != nil {
			// This only happens for errors in configuration & set-up, not for
			// network errors.
			log.Fatalf("Could not register ingester: %v", err)
		}
		ing := setupIngester(chunkStore, cfg.ingesterConfig, router)

		// Setup gRPC server
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.ingesterConfig.GRPCListenPort))
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		grpcServer := grpc.NewServer(
			grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
				cortex_grpc_middleware.ServerLoggingInterceptor(cfg.logSuccess),
				cortex_grpc_middleware.ServerInstrumentInterceptor(requestDuration),
				otgrpc.OpenTracingServerInterceptor(opentracing.GlobalTracer()),
				cortex_grpc_middleware.ServerUserHeaderInterceptor,
			)),
		)
		cortex.RegisterIngesterServer(grpcServer, ing)
		go grpcServer.Serve(lis)
		defer grpcServer.Stop()

		// Deferring a func to make ordering obvious
		defer func() {
			registration.ChangeState(ring.Leaving)
			ing.Stop()
			registration.Unregister()
		}()

		prometheus.MustRegister(registration)

	case modeRuler:
		// XXX: Too much duplication w/ distributor set up.
		cfg.distributorConfig.Ring = r
		cfg.rulerConfig.DistributorConfig = cfg.distributorConfig
		ruler, err := setupRuler(chunkStore, cfg.rulerConfig)
		if err != nil {
			// Some of our initial configuration was fundamentally invalid.
			log.Fatalf("Could not set up ruler: %v", err)
		}
		// XXX: Single-tenanted as part of our initially super hacky way of dogfooding.
		worker := ruler.GetWorkerFor(cfg.rulerConfig.UserID)
		go worker.Run()
		defer worker.Stop()

	default:
		log.Fatalf("Mode %s not supported!", cfg.mode)
	}

	router.Handle("/metrics", prometheus.Handler())
	instrumented := middleware.Merge(
		middleware.Func(func(handler http.Handler) http.Handler {
			return nethttp.Middleware(opentracing.GlobalTracer(), handler)
		}),
		middleware.Log{
			LogSuccess: cfg.logSuccess,
		},
		middleware.Instrument{
			Duration:     requestDuration,
			RouteMatcher: router,
		},
	).Wrap(router)
	go http.ListenAndServe(fmt.Sprintf(":%d", cfg.listenPort), instrumented)

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
	dailyBucketsFrom, err := time.Parse("2006-01-02", cfg.dynamodbDailyBucketsFrom)
	if err != nil {
		return nil, fmt.Errorf("error parsing daily buckets begin date: %v", err)
	}
	chunkStore, err := chunk.NewAWSStore(chunk.StoreConfig{
		S3URL:            cfg.s3URL,
		DynamoDBURL:      cfg.dynamodbURL,
		ChunkCache:       chunkCache,
		DailyBucketsFrom: model.TimeFromUnix(dailyBucketsFrom.Unix()),
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
	cfg distributor.Config,
	chunkStore chunk.Store,
	router *mux.Router,
) {
	dist, err := distributor.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	prometheus.MustRegister(dist)

	router.Path("/push").Handler(http.HandlerFunc(dist.PushHandler))

	// TODO: Move querier to separate binary.
	setupQuerier(dist, chunkStore, router)
}

// setupQuerier sets up a complete querying pipeline:
//
// PromQL -> MergeQuerier -> Distributor -> IngesterQuerier -> Ingester
//              |
//              `----------> ChunkQuerier -> DynamoDB/S3
func setupQuerier(
	distributor *distributor.Distributor,
	chunkStore chunk.Store,
	router *mux.Router,
) {
	queryable := querier.NewQueryable(distributor, chunkStore)
	engine := promql.NewEngine(queryable, nil)
	api := v1.NewAPI(engine, querier.DummyStorage{Queryable: queryable})
	promRouter := route.New(func(r *http.Request) (context.Context, error) {
		userID := r.Header.Get(user.UserIDHeaderName)
		if userID == "" {
			return nil, fmt.Errorf("no %s header", user.UserIDHeaderName)
		}
		return user.WithID(r.Context(), userID), nil
	}).WithPrefix("/api/prom/api/v1")
	api.Register(promRouter)
	router.PathPrefix("/api/v1").Handler(promRouter)
	router.Path("/user_stats").Handler(http.HandlerFunc(distributor.UserStatsHandler))
	router.Path("/graph").Handler(ui.GraphHandler())
	router.PathPrefix("/static/").Handler(ui.StaticAssetsHandler("/api/prom/static/"))
}

func setupIngester(
	chunkStore chunk.Store,
	cfg ingester.Config,
	router *mux.Router,
) *ingester.Ingester {
	ingester, err := ingester.New(cfg, chunkStore)
	if err != nil {
		log.Fatal(err)
	}
	prometheus.MustRegister(ingester)

	// This interface is temporary until rolled out to prod, then we can remove it
	// in favour of the gRPC interface.
	router.Path("/push").Handler(http.HandlerFunc(ingester.PushHandler))
	router.Path("/query").Handler(http.HandlerFunc(ingester.QueryHandler))
	router.Path("/label_values").Handler(http.HandlerFunc(ingester.LabelValuesHandler))
	router.Path("/user_stats").Handler(http.HandlerFunc(ingester.UserStatsHandler))
	router.Path("/ready").Handler(http.HandlerFunc(ingester.ReadinessHandler))
	return ingester
}

// setupRuler sets up a ruler.
func setupRuler(chunkStore chunk.Store, cfg ruler.Config) (*ruler.Ruler, error) {
	return ruler.New(chunkStore, cfg)
}
