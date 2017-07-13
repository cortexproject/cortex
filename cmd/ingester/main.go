package main

import (
	"flag"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"google.golang.org/grpc"

	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/chunk/storage"
	"github.com/weaveworks/cortex/pkg/ingester"
	"github.com/weaveworks/cortex/pkg/ingester/client"
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
		chunkStoreConfig chunk.StoreConfig
		schemaConfig     chunk.SchemaConfig
		storageConfig    storage.Config
		ingesterConfig   ingester.Config
	)
	// Ingester needs to know our gRPC listen port.
	ingesterConfig.ListenPort = &serverConfig.GRPCListenPort
	util.RegisterFlags(&serverConfig, &chunkStoreConfig, &storageConfig,
		&schemaConfig, &ingesterConfig)
	flag.Parse()

	server, err := server.New(serverConfig)
	if err != nil {
		log.Fatalf("Error initializing server: %v", err)
	}
	defer server.Shutdown()

	storageClient, err := storage.NewStorageClient(storageConfig, schemaConfig)
	if err != nil {
		log.Fatalf("Error initializing storage client: %v", err)
	}

	chunkStore, err := chunk.NewStore(chunkStoreConfig, schemaConfig, storageClient)
	if err != nil {
		log.Fatal(err)
	}
	defer chunkStore.Stop()

	ingester, err := ingester.New(ingesterConfig, schemaConfig, chunkStore)
	if err != nil {
		log.Fatal(err)
	}
	prometheus.MustRegister(ingester)
	defer ingester.Shutdown()

	client.RegisterIngesterServer(server.GRPC, ingester)
	server.HTTP.Path("/ready").Handler(http.HandlerFunc(ingester.ReadinessHandler))
	server.HTTP.Path("/flush").Handler(http.HandlerFunc(ingester.FlushHandler))
	server.Run()
}
