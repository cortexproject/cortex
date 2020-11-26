package stats

import "github.com/prometheus/prometheus/storage"

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
func (s *seriesSetTracker) Next() bool {
	hasNext := s.SeriesSet.Next()
	if hasNext {
		s.numSeries++
		return true
	}

	// Added number of series to the stats and reset the counter.
	s.stats.AddSeries(s.numSeries)
	s.numSeries = 0

	return false
}
