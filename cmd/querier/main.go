package main

import (
	"flag"
	"net/http"

	"google.golang.org/grpc"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/web/api/v1"

	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/chunk/storage"
	"github.com/weaveworks/cortex/pkg/distributor"
	"github.com/weaveworks/cortex/pkg/querier"
	"github.com/weaveworks/cortex/pkg/ring"
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
		ringConfig        ring.Config
		distributorConfig distributor.Config
		chunkStoreConfig  chunk.StoreConfig
		schemaConfig      chunk.SchemaConfig
		storageConfig     storage.Config
	)
	util.RegisterFlags(&serverConfig, &ringConfig, &distributorConfig,
		&chunkStoreConfig, &schemaConfig, &storageConfig, util.LogLevel{})
	flag.Parse()

	log.AddHook(promrus.MustNewPrometheusHook())

	r, err := ring.New(ringConfig)
	if err != nil {
		log.Fatalf("Error initializing ring: %v", err)
	}
	defer r.Stop()

	dist, err := distributor.New(distributorConfig, r)
	if err != nil {
		log.Fatalf("Error initializing distributor: %v", err)
	}
	defer dist.Stop()
	prometheus.MustRegister(dist)

	server, err := server.New(serverConfig)
	if err != nil {
		log.Fatalf("Error initializing server: %v", err)
	}
	defer server.Shutdown()
	server.HTTP.Handle("/ring", r)

	storageClient, err := storage.NewStorageClient(storageConfig, schemaConfig)
	if err != nil {
		log.Fatalf("Error initializing storage client: %v", err)
	}

	chunkStore, err := chunk.NewStore(chunkStoreConfig, schemaConfig, storageClient)
	if err != nil {
		log.Fatal(err)
	}
	defer chunkStore.Stop()

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

	subrouter := server.HTTP.PathPrefix("/api/prom").Subrouter()
	subrouter.PathPrefix("/api/v1").Handler(middleware.AuthenticateUser.Wrap(promRouter))
	subrouter.Path("/read").Handler(middleware.AuthenticateUser.Wrap(http.HandlerFunc(sampleQueryable.RemoteReadHandler)))
	subrouter.Path("/validate_expr").Handler(middleware.AuthenticateUser.Wrap(http.HandlerFunc(dist.ValidateExprHandler)))
	subrouter.Path("/user_stats").Handler(middleware.AuthenticateUser.Wrap(http.HandlerFunc(dist.UserStatsHandler)))

	server.Run()
}
