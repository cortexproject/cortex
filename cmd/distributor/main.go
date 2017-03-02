package main

import (
	"flag"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"google.golang.org/grpc"

	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/cortex/distributor"
	"github.com/weaveworks/cortex/ring"
	"github.com/weaveworks/cortex/util"
)

func main() {
	// The pattern for main functions is a series of config objects, which are
	// registered for command line flags, and then a series of components that
	// are instantiated and composed.  Some rules of thumb:
	// - Config types should only contain 'simple' types (ints, strings, urls etc).
	// - Flag validation should be done by the flag; use a flag.Value where
	//   appropriate.
	// - Config types should map 1:1 with a component type.
	// - Config types should define flags with a common prefix.
	// - It's fine to nest configs within configs, but this should match the
	//   nesting of components within components.
	// - Limit as much is possible sharing of configuration between config types.
	//   Where necessary, use a pointer for this - avoid repetition.
	// - Where a nesting of components its not obvious, it's fine to pass
	//   references to other components constructors to compose them.
	// - First argument for a components constructor should be its matching config
	//   object.

	var (
		serverConfig = server.Config{
			MetricsNamespace: "cortex",
			GRPCMiddleware: []grpc.UnaryServerInterceptor{
				middleware.ServerUserHeaderInterceptor,
			},
			HTTPMiddleware: []middleware.Interface{
				middleware.AuthenticateUser,
			},
		}
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
	defer dist.Stop()
	prometheus.MustRegister(dist)

	server, err := server.New(serverConfig)
	if err != nil {
		log.Fatalf("Error initializing server: %v", err)
	}
	server.HTTP.Handle("/ring", r)
	server.HTTP.Handle("/api/prom/push", http.HandlerFunc(dist.PushHandler))
	server.Run()
}
