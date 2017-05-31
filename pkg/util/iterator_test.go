package util

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local"
	"github.com/prometheus/prometheus/storage/metric"
	"github.com/stretchr/testify/require"
)

func TestMergeSeriesIterator_Metric(t *testing.T) {
	sampleMetric := model.Metric{model.MetricNameLabel: "foo"}
	iterator1 := NewSampleStreamIterator(&model.SampleStream{
		Metric: sampleMetric,
		Values: []model.SamplePair{},
	})
	iterator2 := NewSampleStreamIterator(&model.SampleStream{
		Metric: sampleMetric,
		Values: []model.SamplePair{},
	})

	mergeIterator := NewMergeSeriesIterator([]local.SeriesIterator{iterator1, iterator2})

	for _, c := range []struct {
		mergeIterator  MergeSeriesIterator
		expectedMetric metric.Metric
	}{
		{
			mergeIterator:  mergeIterator,
			expectedMetric: metric.Metric{Metric: sampleMetric},
		},
	} {
		metric := c.mergeIterator.Metric()
		require.Equal(t, c.expectedMetric, metric)
	}
}

func TestMergeSeriesIterator_ValueAtOrBeforeTime(t *testing.T) {
	now := model.Now()
	sample1 := model.SamplePair{Timestamp: now, Value: 1}
	sample2 := model.SamplePair{Timestamp: now.Add(1 * time.Second), Value: 2}
	sample3 := model.SamplePair{Timestamp: now.Add(4 * time.Second), Value: 3}
	sample4 := model.SamplePair{Timestamp: now.Add(8 * time.Second), Value: 7}

	sampleMetric := model.Metric{model.MetricNameLabel: "foo"}
	iterator1 := NewSampleStreamIterator(&model.SampleStream{
		Metric: sampleMetric,
		Values: []model.SamplePair{sample1, sample2, sample3},
	})
	iterator2 := NewSampleStreamIterator(&model.SampleStream{
		Metric: sampleMetric,
		Values: []model.SamplePair{sample2, sample4},
	})

	mergeIterator := NewMergeSeriesIterator([]local.SeriesIterator{iterator1, iterator2})

	for _, c := range []struct {
		mergeIterator  MergeSeriesIterator
		timestamp      model.Time
		expectedSample model.SamplePair
	}{
		{
			mergeIterator:  mergeIterator,
			timestamp:      now,
			expectedSample: sample1,
		},
		{
			mergeIterator:  mergeIterator,
			timestamp:      now.Add(1 * time.Second),
			expectedSample: sample2,
		},
		{
			mergeIterator:  mergeIterator,
			timestamp:      now.Add(5 * time.Second),
			expectedSample: sample3,
		},
		{
			mergeIterator:  mergeIterator,
			timestamp:      now.Add(9 * time.Second),
			expectedSample: sample4,
		},
	} {
		sample := c.mergeIterator.ValueAtOrBeforeTime(c.timestamp)
		require.Equal(t, c.expectedSample, sample)
	}
}

func TestMergeSeriesIterator_RangeValues(t *testing.T) {
	now := model.Now()
	sample1 := model.SamplePair{Timestamp: now, Value: 1}
	sample2 := model.SamplePair{Timestamp: now.Add(1 * time.Second), Value: 2}
	sample3 := model.SamplePair{Timestamp: now.Add(4 * time.Second), Value: 3}
	sample4 := model.SamplePair{Timestamp: now.Add(8 * time.Second), Value: 7}

	sampleMetric := model.Metric{model.MetricNameLabel: "foo"}
	iterator1 := NewSampleStreamIterator(&model.SampleStream{
		Metric: sampleMetric,
		Values: []model.SamplePair{sample1, sample2, sample3},
	})
	iterator2 := NewSampleStreamIterator(&model.SampleStream{
		Metric: sampleMetric,
		Values: []model.SamplePair{sample2, sample4},
	})

	mergeIterator := NewMergeSeriesIterator([]local.SeriesIterator{iterator1, iterator2})

	for _, c := range []struct {
		mergeIterator   MergeSeriesIterator
		interval        metric.Interval
		expectedSamples []model.SamplePair
	}{
		{
			mergeIterator:   mergeIterator,
			interval:        metric.Interval{OldestInclusive: now, NewestInclusive: now},
			expectedSamples: []model.SamplePair{sample1},
		},
		{
			mergeIterator:   mergeIterator,
			interval:        metric.Interval{OldestInclusive: now.Add(1 * time.Second), NewestInclusive: now.Add(6 * time.Second)},
			expectedSamples: []model.SamplePair{sample2, sample3},
		},
		{
			mergeIterator:   mergeIterator,
			interval:        metric.Interval{OldestInclusive: now.Add(1 * time.Second), NewestInclusive: now.Add(8 * time.Second)},
			expectedSamples: []model.SamplePair{sample2, sample3, sample4},
		},
		{
			mergeIterator:   mergeIterator,
			interval:        metric.Interval{OldestInclusive: now.Add(9 * time.Second), NewestInclusive: now.Add(9 * time.Second)},
			expectedSamples: nil,
		},
	} {
		samples := c.mergeIterator.RangeValues(c.interval)
		require.Equal(t, c.expectedSamples, samples)
	}
}

func TestSampleStreamIterator_Metric(t *testing.T) {
	sampleMetric := model.Metric{model.MetricNameLabel: "foo"}
	iterator := NewSampleStreamIterator(&model.SampleStream{
		Metric: sampleMetric,
		Values: []model.SamplePair{},
	})
	for _, c := range []struct {
		iterator       SampleStreamIterator
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

func TestSampleStreamIterator_ValueAtOrBeforeTime(t *testing.T) {
	now := model.Now()
	sample1 := model.SamplePair{Timestamp: now, Value: 1}
	sample2 := model.SamplePair{Timestamp: now.Add(1 * time.Second), Value: 2}
	sample3 := model.SamplePair{Timestamp: now.Add(4 * time.Second), Value: 3}
	sample4 := model.SamplePair{Timestamp: now.Add(8 * time.Second), Value: 7}

	sampleMetric := model.Metric{model.MetricNameLabel: "foo"}
	iterator := NewSampleStreamIterator(&model.SampleStream{
		Metric: sampleMetric,
		Values: []model.SamplePair{sample1, sample2, sample3, sample4},
	})

	for _, c := range []struct {
		iterator       SampleStreamIterator
		timestamp      model.Time
		expectedSample model.SamplePair
	}{
		{
			iterator:       iterator,
			timestamp:      now,
			expectedSample: sample1,
		},
		{
			iterator:       iterator,
			timestamp:      now.Add(2 * time.Second),
			expectedSample: sample2,
		},
		{
			iterator:       iterator,
			timestamp:      now.Add(6 * time.Second),
			expectedSample: sample3,
		},
	} {
		sample := c.iterator.ValueAtOrBeforeTime(c.timestamp)
		require.Equal(t, c.expectedSample, sample)
	}
}

func TestSampleStreamIterator_RangeValues(t *testing.T) {
	now := model.Now()
	sample1 := model.SamplePair{Timestamp: now, Value: 1}
	sample2 := model.SamplePair{Timestamp: now.Add(1 * time.Second), Value: 2}
	sample3 := model.SamplePair{Timestamp: now.Add(4 * time.Second), Value: 3}
	sample4 := model.SamplePair{Timestamp: now.Add(8 * time.Second), Value: 7}

	sampleMetric := model.Metric{model.MetricNameLabel: "foo"}
	iterator := NewSampleStreamIterator(&model.SampleStream{
		Metric: sampleMetric,
		Values: []model.SamplePair{sample1, sample2, sample3, sample4},
	})

	for _, c := range []struct {
		iterator        SampleStreamIterator
		interval        metric.Interval
		expectedSamples []model.SamplePair
	}{
		{
			iterator:        iterator,
			interval:        metric.Interval{OldestInclusive: now, NewestInclusive: now},
			expectedSamples: []model.SamplePair{sample1},
		},
		{
			iterator:        iterator,
			interval:        metric.Interval{OldestInclusive: now.Add(1 * time.Second), NewestInclusive: now.Add(6 * time.Second)},
			expectedSamples: []model.SamplePair{sample2, sample3},
		},
		{
			iterator:        iterator,
			interval:        metric.Interval{OldestInclusive: now.Add(9 * time.Second), NewestInclusive: now.Add(9 * time.Second)},
			expectedSamples: nil,
		},
	} {
		samples := c.iterator.RangeValues(c.interval)
		require.Equal(t, c.expectedSamples, samples)
	}
}
