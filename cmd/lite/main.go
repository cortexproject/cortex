package main

import (
	"flag"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/web/api/v1"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/distributor"
	"github.com/weaveworks/cortex/pkg/ingester"
	"github.com/weaveworks/cortex/pkg/ingester/client"
	"github.com/weaveworks/cortex/pkg/querier"
	"github.com/weaveworks/cortex/pkg/ring"
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
		ingesterConfig    ingester.Config
		schemaConfig      chunk.SchemaConfig
		storageConfig     chunk.StorageClientConfig
		unauthenticated   bool
	)
	// Ingester needs to know our gRPC listen port.
	ingesterConfig.ListenPort = &serverConfig.GRPCListenPort
	util.RegisterFlags(&serverConfig, &chunkStoreConfig, &distributorConfig,
		&ingesterConfig, &schemaConfig, &storageConfig)
	flag.BoolVar(&unauthenticated, "unauthenticated", false, "Set to true to disable multitenancy.")
	flag.Parse()

	server, err := server.New(serverConfig)
	if err != nil {
		log.Fatalf("Error initializing server: %v", err)
	}
	defer server.Shutdown()

	storageClient, err := chunk.NewStorageClient(storageConfig, schemaConfig)
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

	ingester, err := ingester.New(ingesterConfig, schemaConfig, chunkStore)
	if err != nil {
		log.Fatal(err)
	}
	prometheus.MustRegister(ingester)
	defer ingester.Shutdown()

	tableClient, err := chunk.NewDynamoDBTableClient(storageConfig.AWSStorageConfig.DynamoDBConfig)
	if err != nil {
		log.Fatalf("Error initializing DynamoDB table client: %v", err)
	}

	tableManager, err := chunk.NewTableManager(schemaConfig, tableClient)
	if err != nil {
		log.Fatalf("Error initializing DynamoDB table manager: %v", err)
	}
	tableManager.Start()
	defer tableManager.Stop()

	queryable := querier.NewQueryable(dist, chunkStore)
	engine := promql.NewEngine(queryable, nil)
	api := v1.NewAPI(engine, querier.DummyStorage{Queryable: queryable},
		querier.DummyTargetRetriever{}, querier.DummyAlertmanagerRetriever{})
	promRouter := route.New(func(r *http.Request) (context.Context, error) {
		return r.Context(), nil
	}).WithPrefix("/api/prom/api/v1")
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
	subrouter.Path("/read").Handler(activeMiddleware.Wrap(http.HandlerFunc(queryable.Q.RemoteReadHandler)))
	subrouter.Path("/validate_expr").Handler(activeMiddleware.Wrap(http.HandlerFunc(dist.ValidateExprHandler)))
	subrouter.Path("/user_stats").Handler(activeMiddleware.Wrap(http.HandlerFunc(dist.UserStatsHandler)))

	client.RegisterIngesterServer(server.GRPC, ingester)
	server.HTTP.Handle("/ready", http.HandlerFunc(ingester.ReadinessHandler))
	server.HTTP.Handle("/flush", http.HandlerFunc(ingester.FlushHandler))
	server.HTTP.Handle("/ring", r)
	server.HTTP.Handle("/api/prom/push", activeMiddleware.Wrap(http.HandlerFunc(dist.PushHandler)))
	server.Run()
}
