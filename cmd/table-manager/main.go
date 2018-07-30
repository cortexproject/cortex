package main

import (
	"flag"
	"os"

	"github.com/go-kit/kit/log/level"
	"google.golang.org/grpc"

	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/chunk/storage"
	"github.com/weaveworks/cortex/pkg/ingester"
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

		ingesterConfig ingester.Config
		storageConfig  storage.Config
		schemaConfig   chunk.SchemaConfig
	)
	util.RegisterFlags(&ingesterConfig, &serverConfig, &storageConfig, &schemaConfig)
	flag.Parse()

	util.InitLogger(&serverConfig)

	if (schemaConfig.ChunkTables.WriteScale.Enabled ||
		schemaConfig.IndexTables.WriteScale.Enabled ||
		schemaConfig.ChunkTables.InactiveWriteScale.Enabled ||
		schemaConfig.IndexTables.InactiveWriteScale.Enabled) &&
		(storageConfig.AWSStorageConfig.ApplicationAutoScaling.URL == nil && storageConfig.AWSStorageConfig.Metrics.URL == "") {
		level.Error(util.Logger).Log("msg", "WriteScale is enabled but no ApplicationAutoScaling or Metrics URL has been provided")
		os.Exit(1)
	}

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

	server, err := server.New(serverConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing server", "err", err)
		os.Exit(1)
	}
	defer server.Shutdown()

	server.Run()
}
