package main

import (
	"flag"
	"os"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"

	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/chunk/storage"
	"github.com/weaveworks/cortex/pkg/configs/db"
	"github.com/weaveworks/cortex/pkg/distributor"
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
		ringConfig        ring.Config
		distributorConfig distributor.Config
		rulerConfig       ruler.Config
		chunkStoreConfig  chunk.StoreConfig
		schemaConfig      chunk.SchemaConfig
		storageConfig     storage.Config
		configStoreConfig ruler.ConfigStoreConfig
		logLevel          util.LogLevel
	)
	util.RegisterFlags(&serverConfig, &ringConfig, &distributorConfig,
		&rulerConfig, &chunkStoreConfig, &storageConfig, &schemaConfig, &configStoreConfig, &logLevel)
	flag.Parse()

	util.InitLogger(logLevel.AllowedLevel)

	storageClient, err := storage.NewStorageClient(storageConfig, schemaConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing storage client: %v", err)
		os.Exit(1)
	}

	chunkStore, err := chunk.NewStore(chunkStoreConfig, schemaConfig, storageClient)
	if err != nil {
		level.Error(util.Logger).Log("err", err)
		os.Exit(1)
	}
	defer chunkStore.Stop()

	r, err := ring.New(ringConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing ring: %v", err)
		os.Exit(1)
	}
	defer r.Stop()

	dist, err := distributor.New(distributorConfig, r)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing distributor: %v", err)
		os.Exit(1)
	}
	defer dist.Stop()
	prometheus.MustRegister(dist)

	rlr, err := ruler.NewRuler(rulerConfig, dist, chunkStore)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing ruler: %v", err)
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
		level.Error(util.Logger).Log("msg", "error initializing ruler server: %v", err)
		os.Exit(1)
	}
	defer rulerServer.Stop()

	server, err := server.New(serverConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing server: %v", err)
		os.Exit(1)
	}
	defer server.Shutdown()

	// Only serve the API for setting & getting rules configs if the database
	// was provided. Allows for smoother migration. See
	// https://github.com/weaveworks/cortex/issues/619
	if configStoreConfig.DBConfig.URI != "" {
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
