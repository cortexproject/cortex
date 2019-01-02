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
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/distributor"
	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ruler"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/common/tracing"
	"github.com/weaveworks/common/user"
)

var (
	serverConfig      server.Config
	chunkStoreConfig  chunk.StoreConfig
	distributorConfig distributor.Config
	querierConfig     querier.Config
	ingesterConfig    ingester.Config
	configStoreConfig ruler.ConfigStoreConfig
	rulerConfig       ruler.Config
	schemaConfig      chunk.SchemaConfig
	storageConfig     storage.Config
	tbmConfig         chunk.TableManagerConfig

	ingesterClientConfig client.Config
	limitsConfig         validation.Limits

	unauthenticated bool
)

func main() {
	getConfigsFromCommandLine()

	// Setting the environment variable JAEGER_AGENT_HOST enables tracing
	trace := tracing.NewFromEnv("ingester")
	defer trace.Close()

	util.InitLogger(&serverConfig)

	server, err := server.New(serverConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing server", "err", err)
		os.Exit(1)
	}
	defer server.Shutdown()

	overrides, err := validation.NewOverrides(limitsConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing overrides", "err", err)
		os.Exit(1)
	}
	schemaConfig.Load()
	chunkStore, err := storage.NewStore(storageConfig, chunkStoreConfig, schemaConfig, overrides)
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
	prometheus.MustRegister(r)
	defer r.Stop()

	dist, err := distributor.New(distributorConfig, ingesterClientConfig, overrides, r)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing distributor", "err", err)
		os.Exit(1)
	}
	defer dist.Stop()

	ingester, err := ingester.New(ingesterConfig, ingesterClientConfig, overrides, chunkStore)
	if err != nil {
		level.Error(util.Logger).Log("err", err)
		os.Exit(1)
	}
	defer ingester.Shutdown()

	// Assume the newest config is the one to use
	storeName := schemaConfig.Configs[len(schemaConfig.Configs)-1].Store
	tableClient, err := storage.NewTableClient(storeName, storageConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing DynamoDB table client", "err", err)
		os.Exit(1)
	}

	tableManager, err := chunk.NewTableManager(tbmConfig, schemaConfig, ingesterConfig.MaxChunkAge, tableClient)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing DynamoDB table manager", "err", err)
		os.Exit(1)
	}
	tableManager.Start()
	defer tableManager.Stop()

	queryable, engine := querier.New(querierConfig, dist, chunkStore)

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
		func() v1.TSDBAdmin { return nil }, // Only needed for admin APIs.
		false, // Disable admin APIs.
		util.Logger,
		querier.DummyRulesRetriever{},
		0, 0, // Remote read samples and concurrency limit.
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
	// migration. See https://github.com/cortexproject/cortex/issues/619
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
		activeMiddleware,
	).Wrap(http.HandlerFunc(dist.PushHandler)))
	server.Run()
}

func getConfigsFromCommandLine() {
	serverConfig = server.Config{
		MetricsNamespace: "cortex",
		GRPCMiddleware: []grpc.UnaryServerInterceptor{
			middleware.ServerUserHeaderInterceptor,
		},
	}
	// Ingester needs to know our gRPC listen port.
	ingesterConfig.LifecyclerConfig.ListenPort = &serverConfig.GRPCListenPort
	flagext.RegisterFlags(&serverConfig, &chunkStoreConfig, &distributorConfig, &querierConfig,
		&ingesterConfig, &configStoreConfig, &rulerConfig, &storageConfig, &schemaConfig,
		&ingesterClientConfig, &limitsConfig, &tbmConfig)
	flag.BoolVar(&unauthenticated, "unauthenticated", false, "Set to true to disable multitenancy.")
	flag.Parse()
}
