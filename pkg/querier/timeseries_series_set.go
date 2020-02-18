package querier

import (
	"sort"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

// timeSeriesSeriesSet is a wrapper around a client.TimeSeries slice to implement to SeriesSet interface
type timeSeriesSeriesSet struct {
	ts []client.TimeSeries
	i  int
}

// Timeseries is a type wrapper that implements the storage.Series interface
type Timeseries struct {
	series client.TimeSeries
}

// TimeSeriesSeriesIterator is a wrapper around a client.TimeSeries to implement the SeriesIterator interface
type TimeSeriesSeriesIterator struct {
	ts *Timeseries
	i  int
}

type byTimeSeriesLabels []client.TimeSeries

func (b byTimeSeriesLabels) Len() int      { return len(b) }
func (b byTimeSeriesLabels) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b byTimeSeriesLabels) Less(i, j int) bool {
	return labels.Compare(client.FromLabelAdaptersToLabels(b[i].Labels), client.FromLabelAdaptersToLabels(b[j].Labels)) < 0
}

func newTimeSeriesSeriesSet(series []client.TimeSeries) *timeSeriesSeriesSet {
	sort.Sort(byTimeSeriesLabels(series))
	return &timeSeriesSeriesSet{
		ts: series,
		i:  -1,
	}
}

// Next implements SeriesSet interface
func (t *timeSeriesSeriesSet) Next() bool { t.i++; return t.i < len(t.ts) }

// At implements SeriesSet interface
func (t *timeSeriesSeriesSet) At() storage.Series { return &Timeseries{series: t.ts[t.i]} }

// Err implements SeriesSet interface
func (t *timeSeriesSeriesSet) Err() error { return nil }

// Labels implements the storage.Series interface
func (t *Timeseries) Labels() labels.Labels {
	return client.FromLabelAdaptersToLabels(t.series.Labels)
}

// Iterator implements the storage.Series interface
func (t *Timeseries) Iterator() storage.SeriesIterator {
	return &TimeSeriesSeriesIterator{
		ts: t,
		i:  -1,
	}
}

// Seek implements SeriesIterator interface
func (t *TimeSeriesSeriesIterator) Seek(s int64) bool {
	idx := sort.Search(len(t.ts.series.Samples), func(i int) bool {
		return t.ts.series.Samples[i].TimestampMs >= s
	})

	if idx < len(t.ts.series.Samples) {
		t.i = idx
		return true
	}

	return false
}

// At implements the SeriesIterator interface
func (t *TimeSeriesSeriesIterator) At() (int64, float64) {
	return t.ts.series.Samples[t.i].TimestampMs, t.ts.series.Samples[t.i].Value
}

// Next implements the SeriesIterator interface
func (t *TimeSeriesSeriesIterator) Next() bool { t.i++; return t.i < len(t.ts.series.Samples) }

// Err implements the SeriesIterator interface
func (t *TimeSeriesSeriesIterator) Err() error { return nil }
