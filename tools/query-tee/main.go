package main

import (
	"flag"
	"log"
	"time"

	"github.com/weaveworks/common/logging"
	"github.com/weaveworks/common/server"

	"github.com/cortexproject/cortex/pkg/util"
)

type Config struct {
	ServerAddr         string
	Backends           BackendsConfig
	PreferredBackend   string
	BackendReadTimeout time.Duration
	LogLevel           logging.Level
}

func main() {
	// Parse CLI flags.
	cfg := Config{}
	flag.StringVar(&cfg.ServerAddr, "server-addr", "localhost:80", "The query-tee server listen address.")
	flag.Var(&cfg.Backends, "backend", "Backend endpoint to query (can be specified multiple times).")
	flag.StringVar(&cfg.PreferredBackend, "preferred-backend", "", "The hostname of the preferred backend when selecting the response to send back to the client.")
	flag.DurationVar(&cfg.BackendReadTimeout, "backend-read-timeout", 90*time.Second, "The timeout when reading the response from a backend.")
	cfg.LogLevel.RegisterFlags(flag.CommandLine)
	flag.Parse()

	// Ensure at least 2 backends are specified.
	if len(cfg.Backends) < 2 {
		log.Fatal("At least 2 backends are required")
	}

	util.InitLogger(&server.Config{
		LogLevel: cfg.LogLevel,
	})

	// Run the proxy.
	proxy := NewProxy(cfg)
	if err := proxy.Run(); err != nil {
		log.Fatalf("Server error: %s", err.Error())
	}
}
