package chunk

import (
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/metric"
	"github.com/stretchr/testify/require"
)

func TestLazySeriesIterator_Metric(t *testing.T) {
	sampleMetric := model.Metric{model.MetricNameLabel: "foo"}
	iterator := NewLazySeriesIterator(sampleMetric)
	for _, c := range []struct {
		iterator       LazySeriesIterator
		expectedMetric metric.Metric
	}{
		{
			iterator:       iterator,
			expectedMetric: metric.Metric{Metric: sampleMetric},
		},
	} {
		metric := c.iterator.Metric()
		require.Equal(t, c.expectedMetric, metric)
	}
}

func TestLazySeriesIterator_ValueAtOrBeforeTime(t *testing.T) {
	now := model.Now()
	sampleMetric := model.Metric{model.MetricNameLabel: "foo"}
	iterator := NewLazySeriesIterator(sampleMetric)
	for _, c := range []struct {
		iterator       LazySeriesIterator
		timestamp      model.Time
		expectedSample model.SamplePair
	}{
		{
			iterator:       iterator,
			timestamp:      now,
			expectedSample: model.ZeroSamplePair,
		},
	} {
		sample := c.iterator.ValueAtOrBeforeTime(c.timestamp)
		require.Equal(t, c.expectedSample, sample)
	}
}

func TestLazySeriesIterator_RangeValues(t *testing.T) {
	now := model.Now()
	sampleMetric := model.Metric{model.MetricNameLabel: "foo"}
	iterator := NewLazySeriesIterator(sampleMetric)
	for _, c := range []struct {
		iterator        LazySeriesIterator
		interval        metric.Interval
		expectedSamples []model.SamplePair
	}{
		{
			iterator:        iterator,
			interval:        metric.Interval{OldestInclusive: now, NewestInclusive: now},
			expectedSamples: nil,
		},
	} {
		samples := c.iterator.RangeValues(c.interval)
		require.Equal(t, c.expectedSamples, samples)
	}
}
