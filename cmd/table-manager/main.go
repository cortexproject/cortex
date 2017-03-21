package main

import (
	"flag"

	"github.com/prometheus/common/log"
	"google.golang.org/grpc"

	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/cortex/chunk"
	"github.com/weaveworks/cortex/util"
)

func main() {
	var (
		serverConfig = server.Config{
			MetricsNamespace: "cortex",
			GRPCMiddleware: []grpc.UnaryServerInterceptor{
				middleware.ServerUserHeaderInterceptor,
			},
		}
		// TODO(jml): need a different thing because we want a different interface (dynamo specific, not storage general)
		storageClientConfig = chunk.StorageClientConfig{}
		tableManagerConfig  = chunk.TableManagerConfig{}
	)
	util.RegisterFlags(&serverConfig, &storageClientConfig, &tableManagerConfig)
	flag.Parse()

	storageClient, tableName, err := chunk.NewStorageClient(storageClientConfig)
	if err != nil {
		log.Fatalf("Error initializing client: %v", err)
	}

	tableManager, err := chunk.NewDynamoTableManager(tableManagerConfig, storageClient, tableName)
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
