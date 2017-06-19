package main

import (
	"flag"

	"github.com/prometheus/common/log"
	"google.golang.org/grpc"

	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/cortex/pkg/chunk"
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

		dynamoDBConfig = chunk.DynamoDBConfig{}
		schemaConfig   chunk.SchemaConfig
	)
	util.RegisterFlags(&serverConfig, &dynamoDBConfig, &schemaConfig)
	flag.Parse()

	tableClient, err := chunk.NewDynamoDBTableClient(dynamoDBConfig)
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
