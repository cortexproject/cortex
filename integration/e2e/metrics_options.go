package e2e

import (
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/pkg/labels"
)

var (
	DefaultMetricsOptions = MetricsOptions{
		GetValue: getMetricValue,
	}
)

// GetMetricValueFunc defined the signature of a function used to get the metric value.
type GetMetricValueFunc func(m *io_prometheus_client.Metric) float64

// MetricsOption defined the signature of a function used to manipulate options.
type MetricsOption func(*MetricsOptions)

// MetricsOptions is the structure holding all options.
type MetricsOptions struct {
	GetValue      GetMetricValueFunc
	LabelMatchers []*labels.Matcher
}

// WithMetricCount is an option to get the histogram/summary count as metric value.
func WithMetricCount(opts *MetricsOptions) {
	opts.GetValue = getMetricCount
}

// WithLabelMatchers is an option to filter only matching series.
func WithLabelMatchers(matchers ...*labels.Matcher) MetricsOption {
	return func(opts *MetricsOptions) {
		opts.LabelMatchers = matchers
	}
}

func buildMetricsOptions(opts []MetricsOption) MetricsOptions {
	result := DefaultMetricsOptions
	for _, opt := range opts {
		opt(&result)
	}
	return result
}
