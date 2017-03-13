package main

import (
	"flag"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"google.golang.org/grpc"

	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/cortex"
	"github.com/weaveworks/cortex/chunk"
	"github.com/weaveworks/cortex/ingester"
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
		chunkStoreConfig chunk.StoreConfig
		ingesterConfig   ingester.Config
	)
	// Ingester needs to know our gRPC listen port.
	ingesterConfig.ListenPort = &serverConfig.GRPCListenPort
	util.RegisterFlags(&serverConfig, &chunkStoreConfig, &ingesterConfig)
	flag.Parse()

	chunkStore, err := chunk.NewStore(chunkStoreConfig)
	if err != nil {
		log.Fatal(err)
	}

	server, err := server.New(serverConfig)
	if err != nil {
		log.Fatalf("Error initializing server: %v", err)
	}
	defer server.Shutdown()

	ingester, err := ingester.New(ingesterConfig, chunkStore)
	if err != nil {
		log.Fatal(err)
	}
	prometheus.MustRegister(ingester)
	defer ingester.Shutdown()

	cortex.RegisterIngesterServer(server.GRPC, ingester)
	server.HTTP.Path("/ready").Handler(http.HandlerFunc(ingester.ReadinessHandler))
	server.Run()
}
