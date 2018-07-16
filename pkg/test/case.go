package test

import (
	"context"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
)

// Case is a metric that can query itself.
type Case interface {
	prometheus.Collector

	Query(ctx context.Context, client v1.API, selectors string, start time.Time, duration time.Duration) ([]model.SamplePair, error)
	ExpectedValueAt(time.Time) float64
}
