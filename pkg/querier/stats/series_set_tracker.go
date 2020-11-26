package stats

import (
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type seriesSetTracker struct {
	storage.SeriesSet

	stats     *Stats
	numSeries int
}

func NewSeriesSetTracker(parent storage.SeriesSet, stats *Stats) storage.SeriesSet {
	return &seriesSetTracker{
		SeriesSet: parent,
		stats:     stats,
	}
}

// Next implements storage.SeriesSet.
func (t *seriesSetTracker) Next() bool {
	hasNext := t.SeriesSet.Next()
	if hasNext {
		t.numSeries++
		return true
	}

	// Add number of series to the stats and reset the counter.
	t.stats.AddSeries(t.numSeries)
	t.numSeries = 0

	return false
}

// At implements storage.SeriesSet.
func (t *seriesSetTracker) At() storage.Series {
	return NewSeriesTracker(t.SeriesSet.At(), t.stats)
}

type seriesTracker struct {
	storage.Series

	stats *Stats
}

func NewSeriesTracker(parent storage.Series, stats *Stats) storage.Series {
	return &seriesTracker{
		Series: parent,
		stats:  stats,
	}
}

func (t *seriesTracker) Iterator() chunkenc.Iterator {
	return NewIteratorTracker(t.Series.Iterator(), t.stats)
}

type iteratorTracker struct {
	chunkenc.Iterator

	stats      *Stats
	numSamples int
}

func NewIteratorTracker(parent chunkenc.Iterator, stats *Stats) chunkenc.Iterator {
	return &iteratorTracker{
		Iterator: parent,
		stats:    stats,
	}
}

func (t *iteratorTracker) Next() bool {
	hasNext := t.Iterator.Next()
	if hasNext {
		t.numSamples++
		return true
	}

	// Add number of samples to the stats and reset the counter.
	t.stats.AddSamples(int64(t.numSamples))
	t.numSamples = 0

	return false
}
