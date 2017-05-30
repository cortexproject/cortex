package util

import (
	"sort"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local"
	"github.com/prometheus/prometheus/storage/metric"
)

// MergeSeriesIterator combines SampleStreamIterator
type MergeSeriesIterator struct {
	iterators []local.SeriesIterator
}

// NewMergeSeriesIterator creates a mergeSeriesIterator
func NewMergeSeriesIterator(iterators []local.SeriesIterator) MergeSeriesIterator {
	return MergeSeriesIterator{
		iterators: iterators,
	}
}

// Metric implements the SeriesIterator interface.
func (msit MergeSeriesIterator) Metric() metric.Metric {
	return metric.Metric{Metric: msit.iterators[0].Metric().Metric}
}

// ValueAtOrBeforeTime implements the SeriesIterator interface.
func (msit MergeSeriesIterator) ValueAtOrBeforeTime(ts model.Time) model.SamplePair {
	latest := model.ZeroSamplePair
	for _, it := range msit.iterators {
		v := it.ValueAtOrBeforeTime(ts)
		if v.Timestamp.After(latest.Timestamp) {
			latest = v
		}
	}
	return latest
}

// RangeValues implements the SeriesIterator interface.
func (msit MergeSeriesIterator) RangeValues(in metric.Interval) []model.SamplePair {
	var sampleSets [][]model.SamplePair
	for _, it := range msit.iterators {
		sampleSets = append(sampleSets, it.RangeValues(in))
	}

	samples := MergeNSampleSets(sampleSets...)
	if len(samples) == 0 {
		return nil
	}
	return samples
}

// Close implements the SeriesIterator interface.
func (msit MergeSeriesIterator) Close() {
	for _, it := range msit.iterators {
		it.Close()
	}
}

// SampleStreamIterator is a struct and not just a renamed type because otherwise the Metric
// field and Metric() methods would clash.
type SampleStreamIterator struct {
	ss *model.SampleStream
}

// NewSampleStreamIterator creates a SampleStreamIterator
func NewSampleStreamIterator(ss *model.SampleStream) SampleStreamIterator {
	return SampleStreamIterator{ss: ss}
}

// Metric implements the SeriesIterator interface.
func (it SampleStreamIterator) Metric() metric.Metric {
	return metric.Metric{Metric: it.ss.Metric}
}

// ValueAtOrBeforeTime implements the SeriesIterator interface.
func (it SampleStreamIterator) ValueAtOrBeforeTime(ts model.Time) model.SamplePair {
	// TODO: This is a naive inefficient approach - in reality, queries go mostly
	// linearly through iterators, and we will want to make successive calls to
	// this method more efficient by taking into account the last result index
	// somehow (similarly to how it's done in Prometheus's
	// memorySeriesIterators).
	i := sort.Search(len(it.ss.Values), func(n int) bool {
		return it.ss.Values[n].Timestamp.After(ts)
	})
	if i == 0 {
		return model.SamplePair{Timestamp: model.Earliest}
	}
	return it.ss.Values[i-1]
}

// RangeValues implements the SeriesIterator interface.
func (it SampleStreamIterator) RangeValues(in metric.Interval) []model.SamplePair {
	n := len(it.ss.Values)
	start := sort.Search(n, func(i int) bool {
		return !it.ss.Values[i].Timestamp.Before(in.OldestInclusive)
	})
	end := sort.Search(n, func(i int) bool {
		return it.ss.Values[i].Timestamp.After(in.NewestInclusive)
	})

	if start == n {
		return nil
	}

	return it.ss.Values[start:end]
}

// Close implements the SeriesIterator interface.
func (it SampleStreamIterator) Close() {}
