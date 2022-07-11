package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/common/signals"

	"github.com/cortexproject/cortex/pkg/cortex"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/tools/blocksconvert"
	"github.com/cortexproject/cortex/tools/blocksconvert/builder"
	"github.com/cortexproject/cortex/tools/blocksconvert/cleaner"
	"github.com/cortexproject/cortex/tools/blocksconvert/scanner"
	"github.com/cortexproject/cortex/tools/blocksconvert/scheduler"
)

type Config struct {
	Target       string
	ServerConfig server.Config

	SharedConfig    blocksconvert.SharedConfig
	ScannerConfig   scanner.Config
	BuilderConfig   builder.Config
	SchedulerConfig scheduler.Config
	CleanerConfig   cleaner.Config
}

func main() {
	cfg := Config{}
	flag.StringVar(&cfg.Target, "target", "", "Module to run: Scanner, Scheduler, Builder")
	cfg.SharedConfig.RegisterFlags(flag.CommandLine)
	cfg.ScannerConfig.RegisterFlags(flag.CommandLine)
	cfg.BuilderConfig.RegisterFlags(flag.CommandLine)
	cfg.SchedulerConfig.RegisterFlags(flag.CommandLine)
	cfg.CleanerConfig.RegisterFlags(flag.CommandLine)
	cfg.ServerConfig.RegisterFlags(flag.CommandLine)
	flag.Parse()

	util_log.InitLogger(&cfg.ServerConfig)

	cortex.DisableSignalHandling(&cfg.ServerConfig)
	serv, err := server.New(cfg.ServerConfig)
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "Unable to initialize server", "err", err.Error())
		os.Exit(1)
	}

	cfg.Target = strings.ToLower(cfg.Target)

	registry := prometheus.DefaultRegisterer

	var targetService services.Service
	switch cfg.Target {
	case "scanner":
		targetService, err = scanner.NewScanner(cfg.ScannerConfig, cfg.SharedConfig, util_log.Logger, registry)
	case "builder":
		targetService, err = builder.NewBuilder(cfg.BuilderConfig, cfg.SharedConfig, util_log.Logger, registry)
	case "scheduler":
		targetService, err = scheduler.NewScheduler(cfg.SchedulerConfig, cfg.SharedConfig, util_log.Logger, registry, serv.HTTP, serv.GRPC)
	case "cleaner":
		targetService, err = cleaner.NewCleaner(cfg.CleanerConfig, cfg.SharedConfig, util_log.Logger, registry)
	default:
		err = fmt.Errorf("unknown target")
	}

	if err != nil {
		level.Error(util_log.Logger).Log("msg", "failed to initialize", "err", err)
		os.Exit(1)
	}

	servService := cortex.NewServerService(serv, func() []services.Service {
		return []services.Service{targetService}
	})
	servManager, err := services.NewManager(servService, targetService)
	if err == nil {
		servManager.AddListener(services.NewManagerListener(nil, nil, func(service services.Service) {
			servManager.StopAsync()
		}))

		err = services.StartManagerAndAwaitHealthy(context.Background(), servManager)
	}
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "Unable to start", "err", err.Error())
		os.Exit(1)
	}

	// Setup signal handler and ask service maanger to stop when signal arrives.
	handler := signals.NewHandler(serv.Log)
	go func() {
		handler.Loop()
		servManager.StopAsync()
	}()

	// We only wait for target service. If any other service fails, listener will stop it (via manager)
	if err := targetService.AwaitTerminated(context.Background()); err != nil {
		level.Error(util_log.Logger).Log("msg", cfg.Target+" failed", "err", targetService.FailureCase())
		os.Exit(1)
	}
}
