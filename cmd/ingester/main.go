package main

import (
	"flag"

	"github.com/prometheus/common/log"

	"github.com/weaveworks/cortex"
	"github.com/weaveworks/cortex/chunk"
	"github.com/weaveworks/cortex/ingester"
	"github.com/weaveworks/cortex/ring"
	"github.com/weaveworks/cortex/server"
	"github.com/weaveworks/cortex/util"
)

func main() {
	var (
		serverConfig               server.Config
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

	server := server.New(serverConfig, registration.Ring)
	chunkStore := chunk.NewAWSStore(chunkStoreConfig)
	ingester, err := ingester.New(ingesterConfig, chunkStore)
	if err != nil {
		log.Fatal(err)
	}
	cortex.RegisterIngesterServer(server.GRPC, ingester)

	// Deferring a func to make ordering obvious
	defer func() {
		registration.ChangeState(ring.IngesterState_LEAVING)
		ingester.Stop()
		registration.Unregister()
		server.Stop()
	}()

	server.Run()
}
