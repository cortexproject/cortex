package main

import (
	"flag"
	"math"
	"os"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/common/tracing"

	"github.com/cortexproject/cortex/pkg/querier/correctness"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

var (
	unixStart = time.Unix(0, 0)
)

func main() {
	var (
		serverConfig server.Config
		runnerConfig correctness.RunnerConfig
	)
	flagext.RegisterFlags(&serverConfig, &runnerConfig)
	flag.Parse()

	// Setting the environment variable JAEGER_AGENT_HOST enables tracing
	trace := tracing.NewFromEnv("test-exporter")
	defer trace.Close()

	util.InitLogger(&serverConfig)

	server, err := server.New(serverConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing server", "err", err)
		os.Exit(1)
	}
	defer server.Shutdown()

	runner, err := correctness.NewRunner(runnerConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing runner", "err", err)
		os.Exit(1)
	}
	defer runner.Stop()

	runner.Add(correctness.NewSimpleTestCase("now_seconds", func(t time.Time) float64 {
		return t.Sub(unixStart).Seconds()
	}))

	runner.Add(correctness.NewSimpleTestCase("sine_wave", func(t time.Time) float64 {
		// With a 15-second scrape interval this gives a ten-minute period
		period := 40 * runnerConfig.ScrapeInterval.Seconds()
		radians := float64(t.Unix()) / period * 2 * math.Pi
		return math.Sin(radians)
	}))

	prometheus.MustRegister(runner)
	server.Run()
}
