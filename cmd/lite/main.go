package main

import (
	"flag"
	"net/http"
	"os"

	"github.com/go-kit/kit/log/level"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/web/api/v1"
	"github.com/prometheus/tsdb"
	"google.golang.org/grpc"

	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/common/tracing"
	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/chunk/storage"
	"github.com/weaveworks/cortex/pkg/distributor"
	"github.com/weaveworks/cortex/pkg/ingester"
	"github.com/weaveworks/cortex/pkg/ingester/client"
	"github.com/weaveworks/cortex/pkg/querier"
	"github.com/weaveworks/cortex/pkg/ring"
	"github.com/weaveworks/cortex/pkg/ruler"
	"github.com/weaveworks/cortex/pkg/util"
)

func main() {
	var (
		serverConfig = server.Config{
			MetricsNamespace: "cortex",
			GRPCMiddleware: []grpc.UnaryServerInterceptor{
				middleware.ServerUserHeaderInterceptor,
			},
		}

		chunkStoreConfig  chunk.StoreConfig
		distributorConfig distributor.Config
		querierConfig     querier.Config
		ingesterConfig    ingester.Config
		configStoreConfig ruler.ConfigStoreConfig
		rulerConfig       ruler.Config
		schemaConfig      chunk.SchemaConfig
		storageConfig     storage.Config
		logLevel          util.LogLevel

		unauthenticated bool
	)
	// Ingester needs to know our gRPC listen port.
	ingesterConfig.LifecyclerConfig.ListenPort = &serverConfig.GRPCListenPort
	util.RegisterFlags(&serverConfig, &chunkStoreConfig, &distributorConfig, &querierConfig,
		&ingesterConfig, &configStoreConfig, &rulerConfig, &storageConfig, &schemaConfig, &logLevel)
	flag.BoolVar(&unauthenticated, "unauthenticated", false, "Set to true to disable multitenancy.")
	flag.Parse()
	ingesterConfig.SetClientConfig(distributorConfig.IngesterClientConfig)

	// Setting the environment variable JAEGER_AGENT_HOST enables tracing
	trace := tracing.NewFromEnv("ingester")
	defer trace.Close()

	util.InitLogger(logLevel.AllowedLevel)

	server, err := server.New(serverConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing server", "err", err)
		os.Exit(1)
	}
	defer server.Shutdown()

	storageClient, err := storage.NewStorageClient(storageConfig, schemaConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing storage client", "err", err)
		os.Exit(1)
	}

	chunkStore, err := chunk.NewCompositeStore(chunkStoreConfig, schemaConfig, storageClient)
	if err != nil {
		level.Error(util.Logger).Log("err", err)
		os.Exit(1)
	}
	defer chunkStore.Stop()

	r, err := ring.New(ingesterConfig.LifecyclerConfig.RingConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing ring", "err", err)
		os.Exit(1)
	}
	defer r.Stop()
	ingesterConfig.LifecyclerConfig.KVClient = r.KVClient

	dist, err := distributor.New(distributorConfig, r)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing distributor", "err", err)
		os.Exit(1)
	}
	defer dist.Stop()
	prometheus.MustRegister(dist)

	ingester, err := ingester.New(ingesterConfig, chunkStore)
	if err != nil {
		level.Error(util.Logger).Log("err", err)
		os.Exit(1)
	}
	prometheus.MustRegister(ingester)
	defer ingester.Shutdown()

	tableClient, err := storage.NewTableClient(storageConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing DynamoDB table client", "err", err)
		os.Exit(1)
	}

	tableManager, err := chunk.NewTableManager(schemaConfig, ingesterConfig.MaxChunkAge, tableClient)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing DynamoDB table manager", "err", err)
		os.Exit(1)
	}
	tableManager.Start()
	defer tableManager.Stop()

	queryable, engine := querier.Make(querierConfig, dist, chunkStore)

	if configStoreConfig.ConfigsAPIURL.String() != "" || configStoreConfig.DBConfig.URI != "" {
		rulesAPI, err := ruler.NewRulesAPI(configStoreConfig)
		if err != nil {
			level.Error(util.Logger).Log("msg", "error initializing ruler config store", "err", err)
			os.Exit(1)
		}
		rlr, err := ruler.NewRuler(rulerConfig, engine, queryable, dist)
		if err != nil {
			level.Error(util.Logger).Log("msg", "error initializing ruler", "err", err)
			os.Exit(1)
		}
		defer rlr.Stop()

		rulerServer, err := ruler.NewServer(rulerConfig, rlr, rulesAPI)
		if err != nil {
			level.Error(util.Logger).Log("msg", "error initializing ruler server", "err", err)
			os.Exit(1)
		}
		defer rulerServer.Stop()
	}

	api := v1.NewAPI(
		engine,
		queryable,
		querier.DummyTargetRetriever{},
		querier.DummyAlertmanagerRetriever{},
		func() config.Config { return config.Config{} },
		map[string]string{}, // TODO: include configuration flags
		func(f http.HandlerFunc) http.HandlerFunc { return f },
		func() *tsdb.DB { return nil }, // Only needed for admin APIs.
		false, // Disable admin APIs.
	)
	promRouter := route.New().WithPrefix("/api/prom/api/v1")
	api.Register(promRouter)

	activeMiddleware := middleware.AuthenticateUser
	if unauthenticated {
		activeMiddleware = middleware.Func(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx := user.InjectOrgID(r.Context(), "0")
				next.ServeHTTP(w, r.WithContext(ctx))
			})
		})
	}

	// Only serve the API for setting & getting rules configs if we're not
	// serving configs from the configs API. Allows for smoother
	// migration. See https://github.com/weaveworks/cortex/issues/619
	if configStoreConfig.ConfigsAPIURL.URL == nil {
		a, err := ruler.NewAPIFromConfig(configStoreConfig.DBConfig)
		if err != nil {
			level.Error(util.Logger).Log("msg", "error initializing public rules API", "err", err)
			os.Exit(1)
		}
		a.RegisterRoutes(server.HTTP)
	}

	subrouter := server.HTTP.PathPrefix("/api/prom").Subrouter()
	subrouter.PathPrefix("/api/v1").Handler(activeMiddleware.Wrap(promRouter))
	subrouter.Path("/read").Handler(activeMiddleware.Wrap(querier.RemoteReadHandler(queryable)))
	subrouter.Path("/validate_expr").Handler(activeMiddleware.Wrap(http.HandlerFunc(dist.ValidateExprHandler)))
	subrouter.Path("/user_stats").Handler(activeMiddleware.Wrap(http.HandlerFunc(dist.UserStatsHandler)))

	client.RegisterIngesterServer(server.GRPC, ingester)
	server.HTTP.Handle("/ready", http.HandlerFunc(ingester.ReadinessHandler))
	server.HTTP.Handle("/flush", http.HandlerFunc(ingester.FlushHandler))
	server.HTTP.Handle("/ring", r)
	operationNameFunc := nethttp.OperationNameFunc(func(r *http.Request) string {
		return r.URL.RequestURI()
	})
	server.HTTP.Handle("/api/prom/push", middleware.Merge(
		middleware.Func(func(handler http.Handler) http.Handler {
			return nethttp.Middleware(opentracing.GlobalTracer(), handler, operationNameFunc)
		}),
		middleware.AuthenticateUser,
	).Wrap(http.HandlerFunc(dist.PushHandler)))
	server.Run()
}
