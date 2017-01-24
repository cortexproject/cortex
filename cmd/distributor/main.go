package main

import (
	"flag"
	"net/http"

	"github.com/prometheus/common/log"

	"github.com/weaveworks/cortex/distributor"
	"github.com/weaveworks/cortex/ring"
	"github.com/weaveworks/cortex/server"
	"github.com/weaveworks/cortex/util"
)

func main() {
	var (
		serverConfig      server.Config
		ringConfig        ring.Config
		distributorConfig distributor.Config
	)
	util.RegisterFlags(&serverConfig, &ringConfig, &distributorConfig)
	flag.Parse()

	r, err := ring.New(ringConfig)
	if err != nil {
		log.Fatalf("Error initializing ring: %v", err)
	}
	defer r.Stop()

	dist, err := distributor.New(distributorConfig, r)
	if err != nil {
		log.Fatalf("Error initializing distributor: %v", err)
	}

	server := server.New(serverConfig, r)
	server.HTTP.Handle("/api/prom/push", http.HandlerFunc(dist.PushHandler))
	defer server.Stop()

	server.Run()
}
