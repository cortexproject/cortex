package main

import (
	"flag"
	"os"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/logging"
	"github.com/weaveworks/common/server"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/tools/querytee"
)

type Config struct {
	ServerMetricsPort int
	LogLevel          logging.Level
	ProxyConfig       querytee.ProxyConfig
}

func main() {
	// Parse CLI flags.
	cfg := Config{}
	flag.IntVar(&cfg.ServerMetricsPort, "server.metrics-port", 9900, "The port where metrics are exposed.")
	cfg.LogLevel.RegisterFlags(flag.CommandLine)
	cfg.ProxyConfig.RegisterFlags(flag.CommandLine)
	flag.Parse()

	util.InitLogger(&server.Config{
		LogLevel: cfg.LogLevel,
	})

	// Run the instrumentation server.
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGoCollector())

	i := querytee.NewInstrumentationServer(cfg.ServerMetricsPort, registry)
	if err := i.Start(); err != nil {
		level.Error(util.Logger).Log("msg", "Unable to start instrumentation server", "err", err.Error())
		os.Exit(1)
	}

	// Run the proxy.
	proxy, err := querytee.NewProxy(cfg.ProxyConfig, util.Logger, cortexReadRoutes(), registry)
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

func cortexReadRoutes() []querytee.Route {
	samplesComparator := querytee.NewSamplesComparator()
	return []querytee.Route{
		{Path: "/api/v1/query", RouteName: "api_v1_query", Methods: []string{"GET"}, ResponseComparator: samplesComparator},
		{Path: "/api/v1/query_range", RouteName: "api_v1_query_range", Methods: []string{"GET"}, ResponseComparator: samplesComparator},
		{Path: "/api/v1/labels", RouteName: "api_v1_labels", Methods: []string{"GET"}, ResponseComparator: nil},
		{Path: "/api/v1/label/{name}/values", RouteName: "api_v1_label_name_values", Methods: []string{"GET"}, ResponseComparator: nil},
		{Path: "/api/v1/series", RouteName: "api_v1_series", Methods: []string{"GET"}, ResponseComparator: nil},
		{Path: "/api/v1/metadata", RouteName: "api_v1_metadata", Methods: []string{"GET"}, ResponseComparator: nil},
		{Path: "/api/v1/rules", RouteName: "api_v1_rules", Methods: []string{"GET"}, ResponseComparator: nil},
		{Path: "/api/v1/alerts", RouteName: "api_v1_alerts", Methods: []string{"GET"}, ResponseComparator: nil},
	}
}
