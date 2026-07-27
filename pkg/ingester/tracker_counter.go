package ingester

import (
	"context"
	"maps"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/cortexproject/cortex/pkg/util/validation"
)

// trackerCounter counts active series per configured tracker pattern.
// It increments on series creation and decrements on series deletion,
// similar to labelSetCounter but using PromQL matchers.
type trackerCounter struct {
	mu       sync.Mutex
	counts   map[string]int               // tracker name -> count
	matchers map[string][]*labels.Matcher // tracker name -> compiled matchers
}

func newTrackerCounter() *trackerCounter {
	return &trackerCounter{
		counts:   make(map[string]int),
		matchers: make(map[string][]*labels.Matcher),
	}
}

// updateConfig applies new tracker configuration. If trackers changed, backfills
// counts from the TSDB head index.
func (tc *trackerCounter) updateConfig(ctx context.Context, db *tsdb.DB, trackers validation.ActiveSeriesTrackersConfig) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	newMatchers := make(map[string][]*labels.Matcher, len(trackers))
	for i := range trackers {
		newMatchers[trackers[i].Name] = trackers[i].ParsedMatchers()
	}

	if trackerMatchersEqual(tc.matchers, newMatchers) {
		return
	}

	// Config changed — backfill counts from TSDB head.
	// Keep old counts so updateMetrics can detect and delete stale tracker metrics.
	tc.matchers = newMatchers
	oldCounts := tc.counts
	tc.counts = make(map[string]int, len(newMatchers)+len(oldCounts))
	maps.Copy(tc.counts, oldCounts)

	if db == nil || len(newMatchers) == 0 {
		return
	}

	ir, err := db.Head().Index()
	if err != nil {
		return
	}
	defer ir.Close()

	for name, m := range tc.matchers {
		p, err := tsdb.PostingsForMatchers(ctx, ir, m...)
		if err != nil {
			continue
		}
		count := 0
		for p.Next() {
			count++
		}
		tc.counts[name] = count
	}
}

// increase is called on PostCreation for a new series.
func (tc *trackerCounter) increase(metric labels.Labels) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	for name, m := range tc.matchers {
		if matchesAll(metric, m) {
			tc.counts[name]++
		}
	}
}

// decrease is called on PostDeletion for a removed series.
func (tc *trackerCounter) decrease(metric labels.Labels) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	for name, m := range tc.matchers {
		if matchesAll(metric, m) {
			tc.counts[name]--
		}
	}
}

// updateMetrics reports current counts and cleans up stale tracker metrics.
func (tc *trackerCounter) updateMetrics(gauge *prometheus.GaugeVec, userID string, trackers validation.ActiveSeriesTrackersConfig) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	activeNames := make(map[string]struct{}, len(trackers))
	for _, t := range trackers {
		gauge.WithLabelValues(userID, t.Name).Set(float64(tc.counts[t.Name]))
		activeNames[t.Name] = struct{}{}
	}

	// Delete metrics for trackers no longer in config.
	for name := range tc.counts {
		if _, ok := activeNames[name]; !ok {
			gauge.DeleteLabelValues(userID, name)
			delete(tc.counts, name)
		}
	}
}

func trackerMatchersEqual(a, b map[string][]*labels.Matcher) bool {
	if len(a) != len(b) {
		return false
	}
	for name, am := range a {
		bm, ok := b[name]
		if !ok || len(am) != len(bm) {
			return false
		}
		for i := range am {
			if am[i].String() != bm[i].String() {
				return false
			}
		}
	}
	return true
}
