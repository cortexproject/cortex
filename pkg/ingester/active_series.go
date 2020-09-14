package ingester

import (
	"math"
	"sync"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"

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
	refsMu        sync.Mutex
	refs          map[model.Fingerprint][]activeSeriesEntry
	oldestEntryTs int64 // Unix nanoseconds. Only used and updated by purge. Zero = unknown.
	active        int   // Number of active entries in this stripe. Only decreased during purge.
}

// activeSeriesEntry holds a timestamp for single series.
type activeSeriesEntry struct {
	lbs labels.Labels
	ts  int64 // Unix nanoseconds
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
	s.refsMu.Lock()
	defer s.refsMu.Unlock()

	nanos := now.UnixNano()
	if nanos < s.oldestEntryTs {
		s.oldestEntryTs = 0
	}

	// Check if already exists within the entries.
	for ix, entry := range s.refs[fp] {
		if !labels.Equal(entry.lbs, series) {
			continue
		}

		if nanos > entry.ts {
			entry.ts = nanos
			s.refs[fp][ix] = entry
		}
		return
	}

	// The entry doesn't exist, so we have to add a new one.
	entry := activeSeriesEntry{
		lbs: labelsCopy(series),
		ts:  nanos,
	}
	s.refs[fp] = append(s.refs[fp], entry)
	s.active++
}

func (s *activeSeriesStripe) purge(keepUntil time.Time) {
	s.refsMu.Lock()
	defer s.refsMu.Unlock()

	keepUntilNanos := keepUntil.UnixNano()
	if s.oldestEntryTs > 0 && keepUntilNanos <= s.oldestEntryTs {
		// Nothing to do.
		return
	}

	s.active = 0

	oldest := int64(math.MaxInt64)
	for fp, entries := range s.refs {
		// Since we do expect very few fingerprint collisions, we
		// have an optimized implementation for the common case.
		if len(entries) == 1 {
			if entries[0].ts < keepUntilNanos {
				delete(s.refs, fp)
				continue
			}

			s.active++
			if entries[0].ts < oldest {
				oldest = entries[0].ts
			}
			continue
		}

		// We have more entries, which means there's a collision,
		// so we have to iterate over the entries.
		for i := 0; i < len(entries); {
			if entries[i].ts < keepUntilNanos {
				entries = append(entries[:i], entries[i+1:]...)
			} else {
				if entries[i].ts < oldest {
					oldest = entries[i].ts
				}

				i++
			}
		}

		// Either update or delete the entries in the map
		if cnt := len(entries); cnt == 0 {
			delete(s.refs, fp)
		} else {
			s.active += cnt
			s.refs[fp] = entries
		}
	}

	if oldest == math.MaxInt64 {
		s.oldestEntryTs = 0
	} else {
		s.oldestEntryTs = oldest
	}
}

func (s *activeSeriesStripe) getActive() int {
	s.refsMu.Lock()
	defer s.refsMu.Unlock()

	return s.active
}
