package blocksconvert

import (
	"context"
	"flag"
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
	"github.com/cortexproject/cortex/tools/querytee"
)

type Config struct {
	Target            string
	LogLevel          logging.Level
	ServerMetricsPort int
}

func main() {
	cfg := Config{}
	flag.StringVar(&cfg.Target, "target", "", "Module to run: Scanner, Scheduler, Builder")
	flag.IntVar(&cfg.ServerMetricsPort, "server.metrics-port", 9900, "The port where metrics are exposed.")
	cfg.LogLevel.RegisterFlags(flag.CommandLine)
	flag.Parse()

	util.InitLogger(&server.Config{
		LogLevel: cfg.LogLevel,
	})

	cfg.Target = strings.ToLower(cfg.Target)

	var targetService services.Service
	switch cfg.Target {
	case "scanner":
		// run scanner
	case "scheduler":
		// run scheduler
	case "builder":
		// run builder
	}

	if targetService == nil {
		level.Error(util.Logger).Log("msg", "unknown target", "target", cfg.Target)
		os.Exit(1)
	}

	// Run the instrumentation server.
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGoCollector())

	i := querytee.NewInstrumentationServer(cfg.ServerMetricsPort, registry)
	if err := i.Start(); err != nil {
		level.Error(util.Logger).Log("msg", "Unable to start instrumentation server", "err", err.Error())
		os.Exit(1)
	}

	if err := services.StartAndAwaitRunning(context.Background(), targetService); err != nil {
		level.Error(util.Logger).Log("msg", "Unable to start "+cfg.Target, "err", err.Error())
		os.Exit(1)
	}

	// Setup signal handler and stop service gently when asked to stop.
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
