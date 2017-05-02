package main

import (
	"flag"
	"net/http"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/retrieval"
	"github.com/prometheus/prometheus/web/api/v1"

	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/distributor"
	"github.com/weaveworks/cortex/pkg/querier"
	"github.com/weaveworks/cortex/pkg/ring"
	"github.com/weaveworks/cortex/pkg/util"
)

type dummyTargetRetriever struct{}

func (r dummyTargetRetriever) Targets() []*retrieval.Target { return nil }

type dummyAlertmanagerRetriever struct{}

func (r dummyAlertmanagerRetriever) Alertmanagers() []string { return nil }

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
		storageConfig     chunk.StorageClientConfig
	)
	util.RegisterFlags(&serverConfig, &ringConfig, &distributorConfig, &chunkStoreConfig, &storageConfig)
	flag.Parse()

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

	storageClient, err := chunk.NewStorageClient(storageConfig)
	if err != nil {
		log.Fatalf("Error initializing storage client: %v", err)
	}

	chunkStore, err := chunk.NewStore(chunkStoreConfig, storageClient)
	if err != nil {
		log.Fatal(err)
	}
	defer chunkStore.Stop()

	queryable := querier.NewQueryable(dist, chunkStore)
	engine := promql.NewEngine(queryable, nil)
	api := v1.NewAPI(engine, querier.DummyStorage{Queryable: queryable}, dummyTargetRetriever{}, dummyAlertmanagerRetriever{})
	promRouter := route.New(func(r *http.Request) (context.Context, error) {
		return r.Context(), nil
	}).WithPrefix("/api/prom/api/v1")
	api.Register(promRouter)

	subrouter := server.HTTP.PathPrefix("/api/prom").Subrouter()
	subrouter.PathPrefix("/api/v1").Handler(middleware.AuthenticateUser.Wrap(promRouter))
	subrouter.Path("/read").Handler(middleware.AuthenticateUser.Wrap(http.HandlerFunc(queryable.Q.RemoteReadHandler)))
	subrouter.Path("/validate_expr").Handler(middleware.AuthenticateUser.Wrap(http.HandlerFunc(dist.ValidateExprHandler)))
	subrouter.Path("/user_stats").Handler(middleware.AuthenticateUser.Wrap(http.HandlerFunc(dist.UserStatsHandler)))

	server.Run()
}
