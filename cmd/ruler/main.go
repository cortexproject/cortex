package main

import (
	"flag"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"google.golang.org/grpc"

	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/cortex/chunk"
	"github.com/weaveworks/cortex/distributor"
	"github.com/weaveworks/cortex/ring"
	"github.com/weaveworks/cortex/ruler"
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
		ringConfig        ring.Config
		distributorConfig distributor.Config
		rulerConfig       ruler.Config
		chunkStoreConfig  chunk.StoreConfig
		storageConfig     chunk.StorageClientConfig
	)
	util.RegisterFlags(&serverConfig, &ringConfig, &distributorConfig, &rulerConfig, &chunkStoreConfig, &storageConfig)
	flag.Parse()

	storageClient, err := chunk.NewStorageClient(storageConfig)
	if err != nil {
		log.Fatalf("Error initializing storage client: %v", err)
	}

	chunkStore, err := chunk.NewStore(chunkStoreConfig, storageClient)
	if err != nil {
		log.Fatal(err)
	}
	defer chunkStore.Stop()

	r, err := ring.New(ringConfig)
	if err != nil {
		log.Fatalf("Error initializing ring: %v", err)
	}
	defer r.Stop()

	dist, err := distributor.New(distributorConfig, r)
	if err != nil {
		log.Fatalf("Error initializing distributor: %v", err)
	}
	defer dist.Stop()
	prometheus.MustRegister(dist)

	rlr, err := ruler.NewRuler(rulerConfig, dist, chunkStore)
	if err != nil {
		log.Fatalf("Error initializing ruler: %v", err)
	}
	defer rlr.Stop()

	rulerServer, err := ruler.NewServer(rulerConfig, rlr)
	if err != nil {
		log.Fatalf("Error initializing ruler server: %v", err)
	}
	defer rulerServer.Stop()

	server, err := server.New(serverConfig)
	if err != nil {
		log.Fatalf("Error initializing server: %v", err)
	}
	defer server.Shutdown()

	server.HTTP.Handle("/ring", r)
	server.Run()
}
