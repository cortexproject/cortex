package main

import (
	"flag"
	"net/http"
	"os"

	"google.golang.org/grpc"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/web/api/v1"
	"github.com/prometheus/tsdb"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	chunk_util "github.com/cortexproject/cortex/pkg/chunk/util"
	"github.com/cortexproject/cortex/pkg/distributor"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/querier/frontend"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/validation"
	httpgrpc_server "github.com/weaveworks/common/httpgrpc/server"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/common/tracing"
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
		clientConfig      client.Config
		limits            validation.Limits
		querierConfig     querier.Config
		chunkStoreConfig  chunk.StoreConfig
		schemaConfig      chunk.SchemaConfig
		storageConfig     storage.Config
		workerConfig      frontend.WorkerConfig
		queryParallelism  int
	)
	flagext.RegisterFlags(&serverConfig, &ringConfig, &distributorConfig, &clientConfig, &limits,
		&querierConfig, &chunkStoreConfig, &schemaConfig, &storageConfig, &workerConfig)
	flag.IntVar(&queryParallelism, "querier.query-parallelism", 100, "Max subqueries run in parallel per higher-level query.")
	flag.Parse()
	chunk_util.QueryParallelism = queryParallelism

	// Setting the environment variable JAEGER_AGENT_HOST enables tracing
	trace := tracing.NewFromEnv("querier")
	defer trace.Close()

	util.InitLogger(&serverConfig)

	r, err := ring.New(ringConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing ring", "err", err)
		os.Exit(1)
	}
	prometheus.MustRegister(r)
	defer r.Stop()

	overrides, err := validation.NewOverrides(limits)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing overrides", "err", err)
		os.Exit(1)
	}

	dist, err := distributor.New(distributorConfig, clientConfig, overrides, r)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing distributor", "err", err)
		os.Exit(1)
	}
	defer dist.Stop()

	server, err := server.New(serverConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing server", "err", err)
		os.Exit(1)
	}
	defer server.Shutdown()
	server.HTTP.Handle("/ring", r)

	chunkStore, err := storage.NewStore(storageConfig, chunkStoreConfig, schemaConfig, overrides)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing storage client", "err", err)
		os.Exit(1)
	}
	defer chunkStore.Stop()

	worker, err := frontend.NewWorker(workerConfig, httpgrpc_server.NewServer(server.HTTPServer.Handler), util.Logger)
	if err != nil {
		level.Error(util.Logger).Log("err", err)
		os.Exit(1)
	}
	defer worker.Stop()

	queryable, engine := querier.New(querierConfig, dist, chunkStore)
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
		util.Logger,
		querier.DummyRulesRetriever{},
	)
	promRouter := route.New().WithPrefix("/api/prom/api/v1")
	api.Register(promRouter)

	subrouter := server.HTTP.PathPrefix("/api/prom").Subrouter()
	subrouter.PathPrefix("/api/v1").Handler(middleware.AuthenticateUser.Wrap(promRouter))
	subrouter.Path("/read").Handler(middleware.AuthenticateUser.Wrap(querier.RemoteReadHandler(queryable)))
	subrouter.Path("/validate_expr").Handler(middleware.AuthenticateUser.Wrap(http.HandlerFunc(dist.ValidateExprHandler)))
	subrouter.Path("/user_stats").Handler(middleware.AuthenticateUser.Wrap(http.HandlerFunc(dist.UserStatsHandler)))

	server.Run()
}
