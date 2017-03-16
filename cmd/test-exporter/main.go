package main

import (
	"flag"
	"math"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/server"

	"github.com/weaveworks-experiments/prometheus-benchmarks/pkg/test"
)

var (
	listen         = flag.String("listen", ":80", "Address to listen on.")
	scrapeInterval = flag.Duration("scrape-interval", 15*time.Second, "Expected scrape interval")
	unixStart      = time.Unix(0, 0)
)

func main() {
	var serverConfig server.Config
	serverConfig.RegisterFlags(flag.CommandLine)
	flag.Parse()

	server, err := server.New(serverConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer server.Shutdown()

	ts, err := test.NewTestCases()
	if err != nil {
		log.Fatal(err)
	}
	defer ts.Stop()

	ts.Add(test.NewSimpleTestCase("now_seconds", func(t time.Time) float64 {
		return t.Sub(unixStart).Seconds()
	}))

	ts.Add(test.NewSimpleTestCase("sine_wave", func(t time.Time) float64 {
		degrees := t.Sub(unixStart).Seconds() * (float64(*scrapeInterval) / float64(time.Second))
		radians := (degrees * math.Pi) / 180.
		return math.Sin(radians)
	}))

	prometheus.MustRegister(ts)
	server.Run()
}
