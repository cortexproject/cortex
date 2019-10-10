package querysharding

import (
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
)

// needed to map back from api response to the underlying series data
func ResponseToSeries(resp queryrange.Response) (storage.SeriesSet, error) {
	switch resp.ResultType {
	case promql.ValueTypeVector, promql.ValueTypeMatrix:
		return NewSeriesSet(resp.Result), nil
	}

	return nil, errors.Errorf(
		"Invalid promql.Value type: [%s]. Only %s and %s supported",
		resp.ResultType,
		promql.ValueTypeVector,
		promql.ValueTypeMatrix,
	)
}

func NewSeriesSet(results []queryrange.SampleStream) *downstreamSeriesSet {
	set := make([]*downstreamSeries, 0, len(results))
	for _, srcSeries := range results {
		series := &downstreamSeries{
			metric: make([]labels.Label, 0, len(srcSeries.Labels)),
			points: make([]promql.Point, 0, len(srcSeries.Samples)),
		}

		for _, l := range srcSeries.Labels {
			series.metric = append(series.metric, labels.Label(l))
		}

		for _, pt := range srcSeries.Samples {
			series.points = append(series.points, promql.Point{
				T: pt.TimestampMs,
				V: pt.Value,
			})
		}

		set = append(set, series)
	}

	return &downstreamSeriesSet{
		set: set,
	}
}

// downstreamSeriesSet is an in-memory series that's mapped from a promql.Value (vector or matrix)
type downstreamSeriesSet struct {
	i     int
	set   []*downstreamSeries
	begun bool
}

// impls storage.SeriesSet
func (set *downstreamSeriesSet) Next() bool {
	if !set.begun {
		set.begun = true
	} else {
		set.i++
	}

	if set.i >= len(set.set) {
		return false
	}

	return true
}

// impls storage.SeriesSet
func (set *downstreamSeriesSet) At() storage.Series {
	return set.set[set.i]
}

// impls storage.SeriesSet
func (set *downstreamSeriesSet) Err() error {
	return nil
}

type downstreamSeries struct {
	metric labels.Labels
	i      int
	points []promql.Point
	begun  bool
}

// impls storage.Series
// Labels returns the complete set of labels identifying the series.
func (series *downstreamSeries) Labels() labels.Labels {
	return series.metric
}

// impls storage.Series
// Iterator returns a new iterator of the data of the series.
func (series *downstreamSeries) Iterator() storage.SeriesIterator {
	// TODO(owen): unsure if this method should return a new iterator re-indexed to 0 or if it can
	// be a passthrough method. Opting for the former for safety (although it contains the same slice).
	return &downstreamSeries{
		metric: series.metric,
		points: series.points,
	}
}

// impls storage.SeriesIterator
// Seek advances the iterator forward to the value at or after
// the given timestamp.
func (series *downstreamSeries) Seek(t int64) bool {

	// TODO(owen): Is this supposed to automatically advance the iterator or should it return the current
	// series if satisfies t?
	for series.Next() {
		ts, _ := series.At()
		if ts >= t {
			return true
		}
	}

	return false
}

// impls storage.SeriesIterator
// At returns the current timestamp/value pair.
func (series *downstreamSeries) At() (t int64, v float64) {
	pt := series.points[series.i]
	return pt.T, pt.V
}

// impls storage.SeriesIterator
// Next advances the iterator by one.
func (series *downstreamSeries) Next() bool {
	if !series.begun {
		series.begun = true
	} else {
		series.i++
	}

	if series.i >= len(series.points) {
		return false
	}
	return true
}

// impls storage.SeriesIterator
// Err returns the current error.
func (series *downstreamSeries) Err() error {
	return nil
}
