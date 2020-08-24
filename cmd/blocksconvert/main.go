package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/logging"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/common/signals"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/tools/blocksconvert"
	"github.com/cortexproject/cortex/tools/blocksconvert/scanner"
	"github.com/cortexproject/cortex/tools/querytee"
)

type Config struct {
	Target            string
	LogLevel          logging.Level
	ServerMetricsPort int

	SharedConfig  blocksconvert.SharedConfig
	ScannerConfig scanner.Config
}

func main() {
	cfg := Config{}
	flag.StringVar(&cfg.Target, "target", "", "Module to run: Scanner, Scheduler, Builder")
	flag.IntVar(&cfg.ServerMetricsPort, "server.metrics-port", 9900, "The port where metrics are exposed.")
	cfg.SharedConfig.RegisterFlags(flag.CommandLine)
	cfg.ScannerConfig.RegisterFlags(flag.CommandLine)
	cfg.LogLevel.RegisterFlags(flag.CommandLine)
	flag.Parse()

	util.InitLogger(&server.Config{
		LogLevel: cfg.LogLevel,
	})

	cfg.Target = strings.ToLower(cfg.Target)

	// Run the instrumentation server.
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGoCollector())

	var targetService services.Service
	var err error
	switch cfg.Target {
	case "scanner":
		targetService, err = scanner.NewScanner(cfg.ScannerConfig, cfg.SharedConfig, util.Logger, registry)
	default:
		err = fmt.Errorf("unknown target")
	}

	if err != nil {
		level.Error(util.Logger).Log("msg", "failed to initialize", "err", err)
		os.Exit(1)
	}

	i := querytee.NewInstrumentationServer(cfg.ServerMetricsPort, registry)
	if err := i.Start(); err != nil {
		level.Error(util.Logger).Log("msg", "Unable to start instrumentation server", "err", err.Error())
		os.Exit(1)
	}

	if err := services.StartAndAwaitRunning(context.Background(), targetService); err != nil {
		level.Error(util.Logger).Log("msg", "Unable to start "+cfg.Target, "err", err.Error())
		os.Exit(1)
	}

	// Setup signal handler and ask service to stop when signal arrives.
	handler := signals.NewHandler(logging.GoKit(log.With(util.Logger, "caller", log.Caller(4))))
	go func() {
		handler.Loop()
		targetService.StopAsync()
	}()

	if err := targetService.AwaitTerminated(context.Background()); err != nil {
		level.Error(util.Logger).Log("msg", cfg.Target+" failed", "err", err.Error())
		os.Exit(1)
	}
}
