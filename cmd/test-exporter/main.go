package main

import (
	"flag"
	"math"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"github.com/weaveworks/common/server"

	"github.com/weaveworks-experiments/prometheus-benchmarks/pkg/test"
)

var (
	unixStart = time.Unix(0, 0)
)

func main() {
	var (
		serverConfig server.Config
		runnerConfig test.RunnerConfig
	)
	serverConfig.RegisterFlags(flag.CommandLine)
	runnerConfig.RegisterFlags(flag.CommandLine)
	flag.Parse()

	server, err := server.New(serverConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer server.Shutdown()

	runner, err := test.NewRunner(runnerConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer runner.Stop()

	runner.Add(test.NewSimpleTestCase("now_seconds", func(t time.Time) float64 {
		return t.Sub(unixStart).Seconds()
	}))

	runner.Add(test.NewSimpleTestCase("sine_wave", func(t time.Time) float64 {
		degrees := t.Sub(unixStart).Seconds() * (float64(runnerConfig.ScrapeInterval) / float64(time.Second))
		radians := (degrees * math.Pi) / 180.
		return math.Sin(radians)
	}))

	prometheus.MustRegister(runner)
	server.Run()
}
