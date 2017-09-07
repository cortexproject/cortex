package main

import (
	"flag"

	"github.com/prometheus/common/log"
	"google.golang.org/grpc"

	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/chunk/storage"
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

		storageConfig storage.Config
		schemaConfig  chunk.SchemaConfig
	)
	util.RegisterFlags(&serverConfig, &storageConfig, &schemaConfig)
	flag.Parse()

	if (schemaConfig.ChunkTables.WriteScale.Enabled ||
		schemaConfig.IndexTables.WriteScale.Enabled ||
		schemaConfig.ChunkTables.InactiveWriteScale.Enabled ||
		schemaConfig.IndexTables.InactiveWriteScale.Enabled) &&
		storageConfig.ApplicationAutoScaling.URL == nil {
		log.Fatal("WriteScale is enabled but no ApplicationAutoScaling URL has been provided")
	}

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

	server, err := server.New(serverConfig)
	if err != nil {
		log.Fatalf("Error initializing server: %v", err)
	}
	defer server.Shutdown()

	server.Run()
}
