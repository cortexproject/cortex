package ingester

import (
	"math"
	"sync"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
)

const (
	numActiveSeriesStripes = 512
)

// ActiveSeries is keeping track of recently active series for a single tenant.
type ActiveSeries struct {
	stripes [numActiveSeriesStripes]activeSeriesStripe
}

// activeSeriesStripe holds a subset of the series timestamps for a single tenant.
type activeSeriesStripe struct {
	// Unix nanoseconds. Only used by purge. Zero = unknown.
	// Updated in purge and when old timestamp is used when updating series (in this case, oldestEntryTs is updated
	// without holding the lock -- hence the atomic).
	oldestEntryTs atomic.Int64

	mu     sync.RWMutex
	refs   map[model.Fingerprint][]activeSeriesEntry
	active int // Number of active entries in this stripe. Only decreased during purge.
}

// activeSeriesEntry holds a timestamp for single series.
type activeSeriesEntry struct {
	lbs   labels.Labels
	nanos atomic.Int64 // Unix timestamp in nanoseconds.
}

func NewActiveSeries() *ActiveSeries {
	c := &ActiveSeries{}

	// Stripes are pre-allocated so that we only read on them and no lock is required.
	for i := 0; i < numActiveSeriesStripes; i++ {
		c.stripes[i].refs = map[model.Fingerprint][]activeSeriesEntry{}
	}

	return c
}

// Updates series timestamp to 'now'. Function is called to make a copy of labels if entry doesn't exist yet.
func (c *ActiveSeries) UpdateSeries(series labels.Labels, now time.Time, labelsCopy func(labels.Labels) labels.Labels) {
	fp := client.Fingerprint(series)
	stripeID := util.HashFP(fp) % numActiveSeriesStripes

	c.stripes[stripeID].updateSeriesTimestamp(now, series, fp, labelsCopy)
}

// Purge removes expired entries from the cache. This function should be called
// periodically to avoid memory leaks.
func (c *ActiveSeries) Purge(keepUntil time.Time) {
	for s := 0; s < numActiveSeriesStripes; s++ {
		c.stripes[s].purge(keepUntil)
	}
}

func (c *ActiveSeries) Active() int {
	total := 0
	for s := 0; s < numActiveSeriesStripes; s++ {
		total += c.stripes[s].getActive()
	}
	return total
}

func (s *activeSeriesStripe) updateSeriesTimestamp(now time.Time, series labels.Labels, fp model.Fingerprint, labelsCopy func(labels.Labels) labels.Labels) {
	nowNanos := now.UnixNano()

	e := s.findEntryForSeries(fp, series)
	if e == nil {
		e = s.findOrCreateEntryForSeries(fp, series, labelsCopy)
	}

	if prev := e.nanos.Load(); nowNanos > prev {
		if e.nanos.CAS(prev, nowNanos) {
			if prevOldest := s.oldestEntryTs.Load(); nowNanos < prevOldest {
				s.oldestEntryTs.CAS(prevOldest, 0)
			}
		}
	}
}

func (s *activeSeriesStripe) findEntryForSeries(fp model.Fingerprint, series labels.Labels) *activeSeriesEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check if already exists within the entries.
	for ix, entry := range s.refs[fp] {
		if labels.Equal(entry.lbs, series) {
			return &s.refs[fp][ix]
		}
	}

	return nil
}

func (s *activeSeriesStripe) findOrCreateEntryForSeries(fp model.Fingerprint, series labels.Labels, labelsCopy func(labels.Labels) labels.Labels) *activeSeriesEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if already exists within the entries.
	for ix, entry := range s.refs[fp] {
		if labels.Equal(entry.lbs, series) {
			return &s.refs[fp][ix]
		}
	}

	s.active++
	e := activeSeriesEntry{
		lbs: labelsCopy(series),
	}

	ix := len(s.refs[fp])
	s.refs[fp] = append(s.refs[fp], e)

	return &s.refs[fp][ix]
}

func (s *activeSeriesStripe) purge(keepUntil time.Time) {
	keepUntilNanos := keepUntil.UnixNano()
	if oldest := s.oldestEntryTs.Load(); oldest > 0 && keepUntilNanos <= oldest {
		// Nothing to do.
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	active := 0

	oldest := int64(math.MaxInt64)
	for fp, entries := range s.refs {
		// Since we do expect very few fingerprint collisions, we
		// have an optimized implementation for the common case.
		if len(entries) == 1 {
			ts := entries[0].nanos.Load()
			if ts < keepUntilNanos {
				delete(s.refs, fp)
				continue
			}

			active++
			if ts < oldest {
				oldest = ts
			}
			continue
		}

		// We have more entries, which means there's a collision,
		// so we have to iterate over the entries.
		for i := 0; i < len(entries); {
			ts := entries[i].nanos.Load()
			if ts < keepUntilNanos {
				entries = append(entries[:i], entries[i+1:]...)
			} else {
				if ts < oldest {
					oldest = ts
				}

				i++
			}
		}

		// Either update or delete the entries in the map
		if cnt := len(entries); cnt == 0 {
			delete(s.refs, fp)
		} else {
			active += cnt
			s.refs[fp] = entries
		}
	}

	if oldest == math.MaxInt64 {
		s.oldestEntryTs.Store(0)
	} else {
		s.oldestEntryTs.Store(oldest)
	}
	s.active = active
}

func (s *activeSeriesStripe) getActive() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.active
}
