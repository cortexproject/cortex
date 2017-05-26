package chunk

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/metric"
)

// LazySeriesIterator is a struct and not just a renamed type because otherwise the Metric
// field and Metric() methods would clash.
type LazySeriesIterator struct {
	metric model.Metric
}

// NewLazySeriesIterator creates a LazySeriesIterator
func NewLazySeriesIterator(metric model.Metric) LazySeriesIterator {
	return LazySeriesIterator{metric: metric}
}

// Metric implements the SeriesIterator interface.
func (it LazySeriesIterator) Metric() metric.Metric {
	return metric.Metric{Metric: it.metric}
}

// ValueAtOrBeforeTime implements the SeriesIterator interface.
func (it LazySeriesIterator) ValueAtOrBeforeTime(ts model.Time) model.SamplePair {
	// TODO: Support these queries
	return model.ZeroSamplePair
}

// RangeValues implements the SeriesIterator interface.
func (it LazySeriesIterator) RangeValues(in metric.Interval) []model.SamplePair {
	// TODO: Support these queries
	return nil
}

// Close implements the SeriesIterator interface.
func (it LazySeriesIterator) Close() {}
