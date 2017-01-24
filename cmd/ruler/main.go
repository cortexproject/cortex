package main

import (
	"flag"

	"github.com/prometheus/common/log"

	"github.com/weaveworks/cortex/chunk"
	"github.com/weaveworks/cortex/distributor"
	"github.com/weaveworks/cortex/ring"
	"github.com/weaveworks/cortex/ruler"
	"github.com/weaveworks/cortex/server"
	"github.com/weaveworks/cortex/util"
)

func main() {
	var (
		serverConfig      server.Config
		ringConfig        ring.Config
		distributorConfig distributor.Config
		rulerConfig       ruler.Config
		chunkStoreConfig  chunk.StoreConfig
	)
	util.RegisterFlags(&serverConfig, &ringConfig, &distributorConfig, &rulerConfig, &chunkStoreConfig)
	flag.Parse()

	chunkStore := chunk.NewAWSStore(chunkStoreConfig)

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

	rulerServer, err := ruler.NewServer(rulerConfig, ruler.NewRuler(rulerConfig, dist, chunkStore))
	if err != nil {
		log.Fatalf("Error initializing ruler: %v", err)
	}
	go rulerServer.Run()
	defer rulerServer.Stop()

	server := server.New(serverConfig, r)
	defer server.Stop()
	server.Run()
}
