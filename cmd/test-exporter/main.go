package main

import (
	"flag"
	"math"
	"time"

	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/common/tracing"

	"github.com/cortexproject/cortex/pkg/testexporter/correctness"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/log"
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

	log.InitLogger(&serverConfig)

	// Setting the environment variable JAEGER_AGENT_HOST enables tracing
	if trace, err := tracing.NewFromEnv("test-exporter"); err != nil {
		level.Error(log.Logger).Log("msg", "Failed to setup tracing", "err", err.Error())
	} else {
		defer trace.Close()
	}

	server, err := server.New(serverConfig)
	log.CheckFatal("initializing server", err)
	defer server.Shutdown()

	runner, err := correctness.NewRunner(runnerConfig)
	log.CheckFatal("initializing runner", err)
	defer runner.Stop()

	runner.Add(correctness.NewSimpleTestCase("now_seconds", func(t time.Time) float64 {
		return t.Sub(unixStart).Seconds()
	}, runnerConfig.CommonTestConfig))

	runner.Add(correctness.NewSimpleTestCase("sine_wave", func(t time.Time) float64 {
		// With a 15-second scrape interval this gives a ten-minute period
		period := float64(40 * runnerConfig.CommonTestConfig.ScrapeInterval.Nanoseconds())
		radians := float64(t.UnixNano()) / period * 2 * math.Pi
		return math.Sin(radians)
	}, runnerConfig.CommonTestConfig))

	if runnerConfig.EnableDeleteSeriesTest {
		runnerConfig.DeleteSeriesTestConfig.ExtraSelectors = runnerConfig.ExtraSelectors
		runnerConfig.DeleteSeriesTestConfig.PrometheusAddr = runnerConfig.PrometheusAddr
		runnerConfig.DeleteSeriesTestConfig.UserID = runnerConfig.UserID
		runner.Add(correctness.NewDeleteSeriesTest("delete_series", func(t time.Time) float64 {
			return t.Sub(unixStart).Seconds()
		}, runnerConfig.DeleteSeriesTestConfig, runnerConfig.CommonTestConfig))
	}

	prometheus.MustRegister(runner)
	err = server.Run()
	log.CheckFatal("running server", err)
}
