package stats

import (
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type seriesSetTracker struct {
	parent storage.SeriesSet
	stats  *Stats
}

func NewSeriesSetTracker(parent storage.SeriesSet, stats *Stats) storage.SeriesSet {
	return &seriesSetTracker{
		parent: parent,
		stats:  stats,
	}
}

// At implements storage.SeriesSet.
func (t *seriesSetTracker) At() storage.Series {
	return NewSeriesTracker(t.parent.At(), t.stats)
}

// Next implements storage.SeriesSet.
func (t *seriesSetTracker) Next() bool {
	return t.parent.Next()
}

// Err implements storage.SeriesSet.
func (t *seriesSetTracker) Err() error {
	return t.parent.Err()
}

// Warnings implements storage.SeriesSet.
func (t *seriesSetTracker) Warnings() storage.Warnings {
	return t.parent.Warnings()
}

type seriesTracker struct {
	parent storage.Series
	stats  *Stats
}

func NewSeriesTracker(parent storage.Series, stats *Stats) storage.Series {
	return &seriesTracker{
		parent: parent,
		stats:  stats,
	}
}

// Iterator implements storage.Series.
func (t *seriesTracker) Iterator() chunkenc.Iterator {
	return NewIteratorTracker(t.parent.Iterator(), t.stats)
}

// Labels implements storage.Series.
func (t *seriesTracker) Labels() labels.Labels {
	return t.parent.Labels()
}

type iteratorTracker struct {
	parent     chunkenc.Iterator
	stats      *Stats
	numSamples int
}

func NewIteratorTracker(parent chunkenc.Iterator, stats *Stats) chunkenc.Iterator {
	return &iteratorTracker{
		parent: parent,
		stats:  stats,
	}
}

// Next implements chunkenc.Iterator.
func (t *iteratorTracker) Next() bool {
	hasNext := t.parent.Next()
	if hasNext {
		t.numSamples++
		return true
	}

	// Add number of samples to the stats and reset the counter.
	t.stats.AddSamples(int64(t.numSamples))
	t.numSamples = 0

	return false
}

// Seek implements chunkenc.Iterator.
func (t *iteratorTracker) Seek(ts int64) bool {
	return t.parent.Seek(ts)
}

// At implements chunkenc.Iterator.
func (t *iteratorTracker) At() (int64, float64) {
	return t.parent.At()
}

// Err implements chunkenc.Iterator.
func (t *iteratorTracker) Err() error {
	return t.parent.Err()
}
