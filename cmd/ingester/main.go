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
	"github.com/weaveworks/cortex/ring"
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
		ingesterRegistrationConfig ring.IngesterRegistrationConfig
		chunkStoreConfig           chunk.StoreConfig
		ingesterConfig             ingester.Config
	)
	// IngesterRegistrator needs to know our gRPC listen port
	ingesterRegistrationConfig.ListenPort = &serverConfig.GRPCListenPort
	util.RegisterFlags(&serverConfig, &ingesterRegistrationConfig, &chunkStoreConfig, &ingesterConfig)
	flag.Parse()

	registration, err := ring.RegisterIngester(ingesterRegistrationConfig)
	if err != nil {
		log.Fatalf("Could not register ingester: %v", err)
	}
	defer registration.Ring.Stop()

	chunkStore, err := chunk.NewStore(chunkStoreConfig)
	if err != nil {
		log.Fatal(err)
	}

	ingester, err := ingester.New(ingesterConfig, chunkStore, registration.Ring)
	if err != nil {
		log.Fatal(err)
	}
	prometheus.MustRegister(ingester)

	server, err := server.New(serverConfig)
	if err != nil {
		log.Fatalf("Error initializing server: %v", err)
	}
	cortex.RegisterIngesterServer(server.GRPC, ingester)
	server.HTTP.Handle("/ring", registration.Ring)
	server.HTTP.Path("/ready").Handler(http.HandlerFunc(ingester.ReadinessHandler))
	server.Run()

	// Shutdown order is important!
	registration.ChangeState(ring.LEAVING)
	ingester.Stop()
	registration.Unregister()
	server.Shutdown()
}
