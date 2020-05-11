package main

import (
	"flag"
	"os"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/logging"
	"github.com/weaveworks/common/server"

	"github.com/cortexproject/cortex/pkg/util"
)

type Config struct {
	ServerServicePort  int
	ServerMetricsPort  int
	BackendEndpoints   string
	PreferredBackend   string
	BackendReadTimeout time.Duration
	LogLevel           logging.Level
}

func main() {
	// Parse CLI flags.
	cfg := Config{}
	flag.IntVar(&cfg.ServerServicePort, "server.service-port", 80, "The port where the query-tee service listens to.")
	flag.IntVar(&cfg.ServerMetricsPort, "server.metrics-port", 9900, "The port where metrics are exposed.")
	flag.StringVar(&cfg.BackendEndpoints, "backend.endpoints", "", "Comma separated list of backend endpoints to query.")
	flag.StringVar(&cfg.PreferredBackend, "backend.preferred", "", "The hostname of the preferred backend when selecting the response to send back to the client.")
	flag.DurationVar(&cfg.BackendReadTimeout, "backend.read-timeout", 90*time.Second, "The timeout when reading the response from a backend.")
	cfg.LogLevel.RegisterFlags(flag.CommandLine)
	flag.Parse()

	util.InitLogger(&server.Config{
		LogLevel: cfg.LogLevel,
	})

	// Run the instrumentation server.
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGoCollector())

	i := NewInstrumentationServer(cfg.ServerMetricsPort, registry)
	if err := i.Start(); err != nil {
		level.Error(util.Logger).Log("msg", "Unable to start instrumentation server", "err", err.Error())
		os.Exit(1)
	}

	// Run the proxy.
	proxy, err := NewProxy(cfg, util.Logger, registry)
	if err != nil {
		level.Error(util.Logger).Log("msg", "Unable to initialize the proxy", "err", err.Error())
		os.Exit(1)
	}

	if err := proxy.Start(); err != nil {
		level.Error(util.Logger).Log("msg", "Unable to start the proxy", "err", err.Error())
		os.Exit(1)
	}

	proxy.Await()
}
