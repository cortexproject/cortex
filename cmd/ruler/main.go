package main

import (
	"flag"
	"os"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/distributor"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ruler"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/validation"
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

		rulerConfig       ruler.Config
		chunkStoreConfig  chunk.StoreConfig
		schemaConfig      chunk.SchemaConfig
		storageConfig     storage.Config
		configStoreConfig ruler.ConfigStoreConfig
		querierConfig     querier.Config
	)

	// Setting the environment variable JAEGER_AGENT_HOST enables tracing
	trace := tracing.NewFromEnv("ruler")
	defer trace.Close()

	util.RegisterFlags(&serverConfig, &ringConfig, &distributorConfig, &clientConfig, &limits,
		&rulerConfig, &chunkStoreConfig, &storageConfig, &schemaConfig, &configStoreConfig,
		&querierConfig)
	flag.Parse()

	util.InitLogger(&serverConfig)

	storageOpts, err := storage.Opts(storageConfig, schemaConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing storage client", "err", err)
		os.Exit(1)
	}
	chunkStore, err := chunk.NewStore(chunkStoreConfig, schemaConfig, storageOpts)
	if err != nil {
		level.Error(util.Logger).Log("err", err)
		os.Exit(1)
	}
	defer chunkStore.Stop()

	r, err := ring.New(ringConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing ring", "err", err)
		os.Exit(1)
	}
	prometheus.MustRegister(r)
	defer r.Stop()

	dist, err := distributor.New(distributorConfig, clientConfig, limits, r)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing distributor", "err", err)
		os.Exit(1)
	}
	defer dist.Stop()

	querierConfig.MaxConcurrent = rulerConfig.NumWorkers
	querierConfig.Timeout = rulerConfig.GroupTimeout
	queryable, engine := querier.New(querierConfig, dist, chunkStore)
	rlr, err := ruler.NewRuler(rulerConfig, engine, queryable, dist)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing ruler", "err", err)
		os.Exit(1)
	}
	defer rlr.Stop()

	rulesAPI, err := ruler.NewRulesAPI(configStoreConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing rules API", "err", err)
		os.Exit(1)
	}

	rulerServer, err := ruler.NewServer(rulerConfig, rlr, rulesAPI)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing ruler server", "err", err)
		os.Exit(1)
	}
	defer rulerServer.Stop()

	server, err := server.New(serverConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing server", "err", err)
		os.Exit(1)
	}
	defer server.Shutdown()

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

	server.HTTP.Handle("/ring", r)
	server.Run()
}
