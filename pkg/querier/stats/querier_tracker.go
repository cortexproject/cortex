package stats

import (
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

type querierTracker struct {
	storage.Querier

	stats *Stats
}

func NewQuerierTracker(parent storage.Querier, stats *Stats) storage.Querier {
	return &querierTracker{
		Querier: parent,
		stats:   stats,
	}
}

// Select implements storage.Querier.
func (t *querierTracker) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	return NewSeriesSetTracker(t.Querier.Select(sortSeries, hints, matchers...), t.stats)
}
