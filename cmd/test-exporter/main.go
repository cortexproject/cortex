package main

import (
	"flag"
	"math"
	"time"

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
	util.CheckFatal("initializing server", err)
	defer server.Shutdown()

	runner, err := correctness.NewRunner(runnerConfig)
	util.CheckFatal("initializing runner", err)
	defer runner.Stop()

	runner.Add(correctness.NewSimpleTestCase("now_seconds", func(t time.Time) float64 {
		return t.Sub(unixStart).Seconds()
	}))

	runner.Add(correctness.NewSimpleTestCase("sine_wave", func(t time.Time) float64 {
		// With a 15-second scrape interval this gives a ten-minute period
		period := float64(40 * runnerConfig.ScrapeInterval.Nanoseconds())
		radians := float64(t.UnixNano()) / period * 2 * math.Pi
		return math.Sin(radians)
	}))

	prometheus.MustRegister(runner)
	server.Run()
}
