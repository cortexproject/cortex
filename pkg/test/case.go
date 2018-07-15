package test

import (
	"context"
	"time"

	api "github.com/prometheus/client_golang/api/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
)

// Case is a metric that can query itself.
type Case interface {
	prometheus.Collector

	Query(ctx context.Context, client api.QueryAPI, selectors string, start time.Time, duration time.Duration) ([]model.SamplePair, error)
	ExpectedValueAt(time.Time) float64
}
