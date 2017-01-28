package main

import (
	"flag"

	"github.com/prometheus/common/log"

	"github.com/weaveworks/cortex/chunk"
	"github.com/weaveworks/cortex/server"
	"github.com/weaveworks/cortex/util"
)

func main() {
	var (
		serverConfig       = server.Config{}
		tableManagerConfig = chunk.TableManagerConfig{}
	)
	util.RegisterFlags(&serverConfig, &tableManagerConfig)
	flag.Parse()

	// Have to initialise server first, as its sets up tracing.
	server := server.New(serverConfig, nil)

	tableManager, err := chunk.NewDynamoTableManager(tableManagerConfig)
	if err != nil {
		log.Fatalf("Error initializing DynamoDB table manager: %v", err)
	}
	tableManager.Start()
	defer tableManager.Stop()

	defer server.Stop()
	server.Run()
}
