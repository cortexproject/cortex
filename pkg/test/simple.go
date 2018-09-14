package test

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"
)

const (
	namespace = "prometheus"
	subsystem = "test_exporter"
)

type simpleTestCase struct {
	prometheus.GaugeFunc
	name            string
	expectedValueAt func(time.Time) float64
}

// NewSimpleTestCase makes a new simpleTestCase
func NewSimpleTestCase(name string, f func(time.Time) float64) Case {
	return &simpleTestCase{
		GaugeFunc: prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Namespace: namespace,
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

func (tc *simpleTestCase) Query(ctx context.Context, client v1.API, selectors string, start time.Time, duration time.Duration) ([]model.SamplePair, error) {
	metricName := prometheus.BuildFQName(namespace, subsystem, tc.name)
	query := fmt.Sprintf("%s{%s}[%dm]", metricName, selectors, duration/time.Minute)
	log.Println(query, "@", start)

	value, err := client.Query(ctx, query, start)
	if err != nil {
		return nil, err
	}
	if value.Type() != model.ValMatrix {
		return nil, fmt.Errorf("didn't get matrix from Prom")
	}

	ms, ok := value.(model.Matrix)
	if !ok {
		return nil, fmt.Errorf("didn't get matrix from Prom")
	}

	result := []model.SamplePair{}
	for _, stream := range ms {
		for _, pair := range stream.Values {
			result = append(result, pair)
		}
	}
	return result, nil
}

func (tc *simpleTestCase) Quantized(duration time.Duration) time.Duration {
	return duration.Truncate(time.Minute)
}
