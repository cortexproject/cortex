package chunk

import (
	"context"
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local/chunk"
	"github.com/prometheus/prometheus/storage/metric"
)

// LazySeriesIterator is a struct and not just a renamed type because otherwise the Metric
// field and Metric() methods would clash.
type LazySeriesIterator struct {
	chunkStore   *Store
	chunks       []Chunk
	chunksLoaded bool

	chunkIt chunk.Iterator

	metric   model.Metric
	from     model.Time
	through  model.Time
	matchers []*metric.LabelMatcher
}

// NewLazySeriesIterator creates a LazySeriesIterator
func NewLazySeriesIterator(chunkStore *Store, metric model.Metric, from model.Time, through model.Time, matchers []*metric.LabelMatcher) LazySeriesIterator {
	return LazySeriesIterator{
		chunkStore: chunkStore,
		metric:     metric,
		from:       from,
		through:    through,
		matchers:   matchers,
	}
}

// Metric implements the SeriesIterator interface.
func (it LazySeriesIterator) Metric() metric.Metric {
	return metric.Metric{Metric: it.metric}
}

// ValueAtOrBeforeTime implements the SeriesIterator interface.
func (it LazySeriesIterator) ValueAtOrBeforeTime(t model.Time) model.SamplePair {
	if !it.chunksLoaded {
		err := it.lookupChunks()
		if err != nil {
			// TODO: handle error
			return model.ZeroSamplePair
		}
	}

	if len(it.chunks) == 0 {
		return model.ZeroSamplePair
	}

	return model.ZeroSamplePair
}

// RangeValues implements the SeriesIterator interface.
func (it LazySeriesIterator) RangeValues(in metric.Interval) []model.SamplePair {
	// TODO: Support these queries
	return nil
}

// Close implements the SeriesIterator interface.
func (it LazySeriesIterator) Close() {}

func (it *LazySeriesIterator) lookupChunks() error {
	metricName, ok := it.metric[model.MetricNameLabel]
	if !ok {
		return fmt.Errorf("series does not have a metric name")
	}

	ctx := context.Background()
	chunks, err := it.chunkStore.lookupChunksByMetricName(ctx, it.from, it.through, it.matchers, metricName)
	if err != nil {
		return fmt.Errorf("")
	}

	it.chunks = chunks
	it.chunksLoaded = true
	return nil
}
