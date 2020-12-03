package stats

import (
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/querier/series"
)

func TestSeriesSetTracker_ShouldTrackStats(t *testing.T) {
	set := series.NewConcreteSeriesSet([]storage.Series{
		newMockSeries(
			labels.New(labels.Label{Name: labels.MetricName, Value: "metric_1"}),
			[]mockSample{{1, 1}, {2, 4}},
		),
		newMockSeries(
			labels.New(labels.Label{Name: labels.MetricName, Value: "metric_2"}),
			[]mockSample{{1, 2}, {2, 5}},
		),
		newMockSeries(
			labels.New(labels.Label{Name: labels.MetricName, Value: "metric_2"}),
			[]mockSample{{1, 3}, {2, 6}},
		),
	})

	// Wrap the original series set with the tracker.
	stats := Stats{}
	set = NewSeriesSetTracker(set, &stats)

	for set.Next() {
		for it := set.At().Iterator(); it.Next(); {
			it.At()
		}
	}

	require.NoError(t, set.Err())
	require.Equal(t, int64(6), stats.GetSamples())
}

type mockSample struct {
	t int64
	v float64
}

type mockSeries struct {
	labels  labels.Labels
	samples []mockSample
}

func newMockSeries(lbls labels.Labels, samples []mockSample) storage.Series {
	return &mockSeries{
		labels:  lbls,
		samples: samples,
	}
}

func (s *mockSeries) Iterator() chunkenc.Iterator {
	return newMockIterator(s.samples)
}

func (s *mockSeries) Labels() labels.Labels {
	return s.labels
}

type mockIterator struct {
	samples []mockSample
	idx     int
}

func newMockIterator(samples []mockSample) chunkenc.Iterator {
	return &mockIterator{
		samples: samples,
		idx:     -1,
	}
}

func (i *mockIterator) Next() bool {
	i.idx++
	return i.idx < len(i.samples)
}

func (i *mockIterator) Seek(_ int64) bool {
	panic("Seek() not implemented")
}

func (i *mockIterator) At() (int64, float64) {
	if i.idx < 0 || i.idx >= len(i.samples) {
		return 0, 0
	}

	return i.samples[i.idx].t, i.samples[i.idx].v
}

func (i *mockIterator) Err() error {
	return nil
}
