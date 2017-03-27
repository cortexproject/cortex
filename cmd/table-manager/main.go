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
		dynamoTableClientConfig = chunk.DynamoTableClientConfig{}
		tableManagerConfig      = chunk.TableManagerConfig{}
	)
	util.RegisterFlags(&serverConfig, &dynamoTableClientConfig, &tableManagerConfig)
	flag.Parse()

	dynamoClient, err := chunk.NewDynamoTableClient(dynamoTableClientConfig)
	if err != nil {
		log.Fatalf("Error initializing DynamoDB client: %v", err)
	}

	tableManager, err := chunk.NewDynamoTableManager(tableManagerConfig, dynamoClient)
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
