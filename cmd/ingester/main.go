package main

import (
	"flag"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"

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

	server := server.New(serverConfig)
	chunkStore, err := chunk.NewStore(chunkStoreConfig)
	if err != nil {
		log.Fatal(err)
	}

	ingester, err := ingester.New(ingesterConfig, chunkStore, registration.Ring)
	if err != nil {
		log.Fatal(err)
	}
	prometheus.MustRegister(ingester)
	cortex.RegisterIngesterServer(server.GRPC, ingester)
	server.HTTP.Handle("/ring", registration.Ring)
	server.HTTP.Path("/ready").Handler(http.HandlerFunc(ingester.ReadinessHandler))

	// Deferring a func to make ordering obvious
	defer func() {
		registration.ChangeState(ring.LEAVING)
		ingester.Stop()
		registration.Unregister()
		server.Stop()
	}()

	server.Run()
}
