package test

import (
	"flag"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	subsystem = "test_exporter"
)

var (
	namespace = flag.String("namespace", "prometheus", "Namespace of metrics.")
)

type simpleTestCase struct {
	prometheus.GaugeFunc
	name            string
	expectedValueAt func(time.Time) float64
}

func NewSimpleTestCase(name string, f func(time.Time) float64) TestCase {
	return &simpleTestCase{
		GaugeFunc: prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Namespace: *namespace,
				Subsystem: subsystem,
				Name:      name,
				Help:      name,
			},
			func() float64 {
				return f(time.Now())
			},
		),
		name:            name,
		expectedValueAt: f,
	}
}

func (tc *simpleTestCase) ExpectedValueAt(t time.Time) float64 {
	return tc.expectedValueAt(t)
}

func (tc *simpleTestCase) Query() string {
	return prometheus.BuildFQName(*namespace, subsystem, tc.name)
}
