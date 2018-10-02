package main

import (
	"flag"
	"math"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"github.com/weaveworks/common/server"

	"github.com/cortexproject/cortex/pkg/querier/correctness"
)

var (
	unixStart = time.Unix(0, 0)
)

func main() {
	var (
		serverConfig server.Config
		runnerConfig correctness.RunnerConfig
	)
	serverConfig.RegisterFlags(flag.CommandLine)
	runnerConfig.RegisterFlags(flag.CommandLine)
	flag.Parse()

	server, err := server.New(serverConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer server.Shutdown()

	runner, err := correctness.NewRunner(runnerConfig)
	if err != nil {
		log.Fatal(err)
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
