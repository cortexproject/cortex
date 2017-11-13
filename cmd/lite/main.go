package main

import (
	"flag"
	"net/http"

	"github.com/opentracing-contrib/go-stdlib/nethttp"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/web/api/v1"
	"google.golang.org/grpc"

	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/chunk/storage"
	"github.com/weaveworks/cortex/pkg/distributor"
	"github.com/weaveworks/cortex/pkg/ingester"
	"github.com/weaveworks/cortex/pkg/ingester/client"
	"github.com/weaveworks/cortex/pkg/querier"
	"github.com/weaveworks/cortex/pkg/ring"
	"github.com/weaveworks/cortex/pkg/ruler"
	"github.com/weaveworks/cortex/pkg/tracing"
	"github.com/weaveworks/cortex/pkg/util"
	"github.com/weaveworks/promrus"
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
		ingesterConfig    ingester.Config
		rulerConfig       ruler.Config
		schemaConfig      chunk.SchemaConfig
		storageConfig     storage.Config

		unauthenticated bool
	)
	// Ingester needs to know our gRPC listen port.
	ingesterConfig.ListenPort = &serverConfig.GRPCListenPort
	util.RegisterFlags(&serverConfig, &chunkStoreConfig, &distributorConfig,
		&ingesterConfig, &rulerConfig, &storageConfig, &schemaConfig, util.LogLevel{})
	flag.BoolVar(&unauthenticated, "unauthenticated", false, "Set to true to disable multitenancy.")
	flag.Parse()
	schemaConfig.MaxChunkAge = ingesterConfig.MaxChunkAge

	// Setting the environment variable JAEGER_AGENT_HOST enables tracing
	trace := tracing.New("lite")
	defer trace.Close()

	log.AddHook(promrus.MustNewPrometheusHook())

	server, err := server.New(serverConfig)
	if err != nil {
		log.Fatalf("Error initializing server: %v", err)
	}
	defer server.Shutdown()

	storageClient, err := storage.NewStorageClient(storageConfig, schemaConfig)
	if err != nil {
		log.Fatalf("Error initializing storage client: %v", err)
	}

	chunkStore, err := chunk.NewStore(chunkStoreConfig, schemaConfig, storageClient)
	if err != nil {
		log.Fatal(err)
	}
	defer chunkStore.Stop()

	r, err := ring.New(ingesterConfig.RingConfig)
	if err != nil {
		log.Fatalf("Error initializing ring: %v", err)
	}
	defer r.Stop()
	ingesterConfig.KVClient = r.KVClient

	dist, err := distributor.New(distributorConfig, r)
	if err != nil {
		log.Fatalf("Error initializing distributor: %v", err)
	}
	defer dist.Stop()
	prometheus.MustRegister(dist)

	ingester, err := ingester.New(ingesterConfig, chunkStore)
	if err != nil {
		log.Fatal(err)
	}
	prometheus.MustRegister(ingester)
	defer ingester.Shutdown()

	tableClient, err := storage.NewTableClient(storageConfig)
	if err != nil {
		log.Fatalf("Error initializing DynamoDB table client: %v", err)
	}

	tableManager, err := chunk.NewTableManager(schemaConfig, tableClient)
	if err != nil {
		log.Fatalf("Error initializing DynamoDB table manager: %v", err)
	}
	tableManager.Start()
	defer tableManager.Stop()

	if rulerConfig.ConfigsAPIURL.String() != "" {
		rlr, err := ruler.NewRuler(rulerConfig, dist, chunkStore)
		if err != nil {
			log.Fatalf("Error initializing ruler: %v", err)
		}
		defer rlr.Stop()

		rulerServer, err := ruler.NewServer(rulerConfig, rlr)
		if err != nil {
			log.Fatalf("Error initializing ruler server: %v", err)
		}
		defer rulerServer.Stop()
	}

	sampleQueryable := querier.NewQueryable(dist, chunkStore, false)
	metadataQueryable := querier.NewQueryable(dist, chunkStore, true)

	engine := promql.NewEngine(sampleQueryable, nil)
	api := v1.NewAPI(
		engine,
		metadataQueryable,
		querier.DummyTargetRetriever{},
		querier.DummyAlertmanagerRetriever{},
		func() config.Config { return config.Config{} },
		func(f http.HandlerFunc) http.HandlerFunc { return f },
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

	subrouter := server.HTTP.PathPrefix("/api/prom").Subrouter()
	subrouter.PathPrefix("/api/v1").Handler(activeMiddleware.Wrap(promRouter))
	subrouter.Path("/read").Handler(activeMiddleware.Wrap(http.HandlerFunc(sampleQueryable.RemoteReadHandler)))
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
