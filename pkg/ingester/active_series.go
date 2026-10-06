package ingester

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	uatomic "go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/ring"
)

const (
	numActiveSeriesStripes = 512
)

// ringState holds the ring ownership data needed for series ownership checks.
// Stored behind an atomic.Pointer so that readers (hot push path) always see
// a consistent snapshot without lock contention, while the writer (periodic
// updateActiveSeries loop) can swap in a new state atomically.
type ringState struct {
	// tokens is the ring's full sorted token list.
	tokens []uint32
	// ownedPositions is parallel to tokens and marks the positions whose replica
	// set includes this ingester. Both come from Lifecycler.GetOwnedTokenPositions
	// and are immutable.
	ownedPositions []bool
}

// emptyRingState is the zero-value ring state used before any ring data is loaded.
var emptyRingState = &ringState{}

// ActiveSeries is keeping track of recently active series for a single tenant.
type ActiveSeries struct {
	// Ring ownership state. Readers on the push path load atomically;
	// the writer (updateTokens) stores a new pointer on ring changes.
	ring atomic.Pointer[ringState]

	// currFingerprint detects ring changes. It is the fingerprint reported by the
	// lifecycler alongside the ownership bitmap, which changes if and only if
	// ownership may have changed. Only accessed by the updateTokens caller
	// (periodic updateActiveSeries goroutine), so no synchronization needed.
	currFingerprint uint64

	stripes [numActiveSeriesStripes]activeSeriesStripe
}

// activeSeriesStripe holds a subset of the series timestamps for a single tenant.
type activeSeriesStripe struct {
	// Unix nanoseconds. Only used by purge. Zero = unknown.
	// Updated in purge and when old timestamp is used when updating series (in this case, oldestEntryTs is updated
	// without holding the lock -- hence the atomic).
	oldestEntryTs uatomic.Int64

	mu                    sync.RWMutex
	refs                  map[uint64][]activeSeriesEntry
	active                int // Number of active entries in this stripe. Only decreased during purge or clear.
	activeNativeHistogram int // Number of active entries only for Native Histogram in this stripe. Only decreased during purge or clear.
	owned                 int // Number of entries owned by this instance. Decreased during purge, clear, or ring changes.
}

// activeSeriesEntry holds a timestamp for single series.
type activeSeriesEntry struct {
	lbs               labels.Labels
	key               uint32         // Ring token hash for this series (used for ownership checks)
	nanos             *uatomic.Int64 // Unix timestamp in nanoseconds. Needs to be a pointer because we don't store pointers to entries in the stripe.
	isNativeHistogram bool
}

func NewActiveSeries() *ActiveSeries {
	c := &ActiveSeries{}
	c.ring.Store(emptyRingState)

	// Stripes are pre-allocated so that we only read on them and no lock is required.
	for i := range numActiveSeriesStripes {
		c.stripes[i].refs = map[uint64][]activeSeriesEntry{}
	}

	return c
}

// UpdateSeries updates series timestamp to 'now'. The key parameter is the ring token
// for this series (computed via ring.TokenForLabels). When key is 0 or ring tokens are
// not loaded, ownership checking is skipped (backward compatible behavior).
func (c *ActiveSeries) UpdateSeries(series labels.Labels, hash uint64, key uint32, now time.Time, nativeHistogram bool, labelsCopy func(labels.Labels) labels.Labels) {
	stripeID := hash % numActiveSeriesStripes

	// Load ring state atomically — readers on the push path always see a consistent snapshot.
	state := c.ring.Load()
	c.stripes[stripeID].updateSeriesTimestamp(now, series, hash, key, nativeHistogram, labelsCopy, state.tokens, state.ownedPositions)
}

// updateTokens updates the cached ring state. Returns true if ownership changed.
// Only called from the updateActiveSeries goroutine (single writer).
//
// The lifecycler's fingerprint is used rather than a hash of the token list,
// because an instance changing state alters the replica set, and therefore
// ownership, without changing any token.
func (c *ActiveSeries) updateTokens(tokens []uint32, ownedPositions []bool, fingerprint uint64) bool {
	if len(tokens) == 0 || fingerprint == c.currFingerprint {
		return false
	}

	// The slices come from the lifecycler and are never mutated in place, so they
	// can be published to readers directly rather than copied.
	c.ring.Store(&ringState{
		tokens:         tokens,
		ownedPositions: ownedPositions,
	})
	c.currFingerprint = fingerprint

	return true
}

// UpdateMetrics updates the owned series count by re-checking ownership if the ring
// changed, and purges expired entries. Called from updateActiveSeries when OwnedMetrics is enabled.
func (c *ActiveSeries) UpdateMetrics(keepUntil time.Time, tokens []uint32, ownedPositions []bool, fingerprint uint64) {
	tokensChanged := c.updateTokens(tokens, ownedPositions, fingerprint)

	// Load the ring state from the atomic pointer for consistency.
	// Even though we're on the same goroutine that just stored it, reading from
	// the pointer ensures all code paths use the same access pattern.
	state := c.ring.Load()
	for s := range numActiveSeriesStripes {
		c.stripes[s].updateMetrics(keepUntil, tokensChanged, state.tokens, state.ownedPositions)
	}
}

// Purge removes expired entries from the cache. This function should be called
// periodically to avoid memory leaks. Used when OwnedMetrics is disabled.
func (c *ActiveSeries) Purge(keepUntil time.Time) {
	for s := range numActiveSeriesStripes {
		c.stripes[s].purge(keepUntil)
	}
}

// nolint // Linter reports that this method is unused, but it is.
func (c *ActiveSeries) clear() {
	for s := range numActiveSeriesStripes {
		c.stripes[s].clear()
	}
}

func (c *ActiveSeries) Active() int {
	total := 0
	for s := range numActiveSeriesStripes {
		total += c.stripes[s].getActive()
	}
	return total
}

// Owned returns the number of active series owned by this instance.
// Returns the same as Active() if ring tokens haven't been loaded yet.
func (c *ActiveSeries) Owned() int {
	total := 0
	for s := range numActiveSeriesStripes {
		total += c.stripes[s].getOwned()
	}
	return total
}

func (c *ActiveSeries) ActiveNativeHistogram() int {
	total := 0
	for s := range numActiveSeriesStripes {
		total += c.stripes[s].getActiveNativeHistogram()
	}
	return total
}

func (s *activeSeriesStripe) updateSeriesTimestamp(now time.Time, series labels.Labels, fingerprint uint64, key uint32, nativeHistogram bool, labelsCopy func(labels.Labels) labels.Labels, tokens []uint32, ownedPositions []bool) {
	nowNanos := now.UnixNano()

	e := s.findEntryForSeries(fingerprint, series)
	entryTimeSet := false
	if e == nil {
		e, entryTimeSet = s.findOrCreateEntryForSeries(fingerprint, key, series, nowNanos, nativeHistogram, labelsCopy, tokens, ownedPositions)
		if e == nil {
			return // Series not owned by this instance, skip tracking
		}
	}

	if !entryTimeSet {
		if prev := e.Load(); nowNanos > prev {
			entryTimeSet = e.CompareAndSwap(prev, nowNanos)
		}
	}

	if entryTimeSet {
		for prevOldest := s.oldestEntryTs.Load(); nowNanos < prevOldest; {
			// If recent purge already removed entries older than "oldest entry timestamp", setting this to 0 will make
			// sure that next purge doesn't take the shortcut route.
			if s.oldestEntryTs.CompareAndSwap(prevOldest, 0) {
				break
			}
		}
	}
}

func (s *activeSeriesStripe) findEntryForSeries(fingerprint uint64, series labels.Labels) *uatomic.Int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check if already exists within the entries.
	for ix, entry := range s.refs[fingerprint] {
		if labels.Equal(entry.lbs, series) {
			return s.refs[fingerprint][ix].nanos
		}
	}

	return nil
}

func (s *activeSeriesStripe) findOrCreateEntryForSeries(fingerprint uint64, key uint32, series labels.Labels, nowNanos int64, nativeHistogram bool, labelsCopy func(labels.Labels) labels.Labels, tokens []uint32, ownedPositions []bool) (*uatomic.Int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if already exists within the entries.
	for ix, entry := range s.refs[fingerprint] {
		if labels.Equal(entry.lbs, series) {
			return s.refs[fingerprint][ix].nanos, false
		}
	}

	// If ring tokens are loaded, check ownership before creating.
	// This prevents tracking series we don't own (e.g., stale distributor routes).
	if !isOwned(key, tokens, ownedPositions) {
		return nil, false
	}

	s.active++
	s.owned++
	if nativeHistogram {
		s.activeNativeHistogram++
	}
	e := activeSeriesEntry{
		lbs:               labelsCopy(series),
		key:               key,
		nanos:             uatomic.NewInt64(nowNanos),
		isNativeHistogram: nativeHistogram,
	}

	s.refs[fingerprint] = append(s.refs[fingerprint], e)

	return e.nanos, true
}

// updateMetrics re-evaluates ownership for all entries when the ring changes,
// and purges expired entries. This combines ownership tracking with the purge cycle.
func (s *activeSeriesStripe) updateMetrics(keepUntil time.Time, tokensChanged bool, tokens []uint32, ownedPositions []bool) {
	keepUntilNanos := keepUntil.UnixNano()
	if oldest := s.oldestEntryTs.Load(); oldest > 0 && keepUntilNanos <= oldest && !tokensChanged {
		// Nothing to do — no expired entries and ring hasn't changed.
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	active := 0
	owned := 0
	activeNativeHistogram := 0
	oldest := int64(math.MaxInt64)

	for fp, entries := range s.refs {
		if len(entries) == 1 {
			// Optimized path for the common case (no fingerprint collision).
			ts := entries[0].nanos.Load()

			// If ring changed and we lost ownership, remove entry.
			if tokensChanged && !isOwned(entries[0].key, tokens, ownedPositions) {
				delete(s.refs, fp)
				continue
			}

			// If expired, remove entry.
			if ts < keepUntilNanos {
				delete(s.refs, fp)
				continue
			}

			active++
			owned++
			if entries[0].isNativeHistogram {
				activeNativeHistogram++
			}
			if ts < oldest {
				oldest = ts
			}
			continue
		}

		// Multiple entries (fingerprint collision) — iterate individually.
		for i := 0; i < len(entries); {
			ts := entries[i].nanos.Load()

			// If ring changed and we lost ownership, remove.
			if tokensChanged && !isOwned(entries[i].key, tokens, ownedPositions) {
				entries = append(entries[:i], entries[i+1:]...)
				continue
			}

			// If expired, remove.
			if ts < keepUntilNanos {
				entries = append(entries[:i], entries[i+1:]...)
				continue
			}

			active++
			owned++
			if entries[i].isNativeHistogram {
				activeNativeHistogram++
			}
			if ts < oldest {
				oldest = ts
			}
			i++
		}

		if len(entries) == 0 {
			delete(s.refs, fp)
		} else {
			s.refs[fp] = entries
		}
	}

	if oldest == math.MaxInt64 {
		s.oldestEntryTs.Store(0)
	} else {
		s.oldestEntryTs.Store(oldest)
	}
	s.active = active
	s.owned = owned
	s.activeNativeHistogram = activeNativeHistogram
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
	activeNativeHistogram := 0

	oldest := int64(math.MaxInt64)
	for fp, entries := range s.refs {
		if len(entries) == 1 {
			ts := entries[0].nanos.Load()
			if ts < keepUntilNanos {
				delete(s.refs, fp)
				continue
			}

			active++
			if entries[0].isNativeHistogram {
				activeNativeHistogram++
			}
			if ts < oldest {
				oldest = ts
			}
			continue
		}

		for i := 0; i < len(entries); {
			ts := entries[i].nanos.Load()
			if ts < keepUntilNanos {
				entries = append(entries[:i], entries[i+1:]...)
			} else {
				if ts < oldest {
					oldest = ts
				}
				active++
				if entries[i].isNativeHistogram {
					activeNativeHistogram++
				}
				i++
			}
		}

		if cnt := len(entries); cnt == 0 {
			delete(s.refs, fp)
		} else {
			s.refs[fp] = entries
		}
	}

	if oldest == math.MaxInt64 {
		s.oldestEntryTs.Store(0)
	} else {
		s.oldestEntryTs.Store(oldest)
	}
	s.active = active
	s.owned = active // When purge is used (flag off), owned == active
	s.activeNativeHistogram = activeNativeHistogram
}

// nolint // Linter reports that this method is unused, but it is.
func (s *activeSeriesStripe) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.oldestEntryTs.Store(0)
	s.refs = map[uint64][]activeSeriesEntry{}
	s.active = 0
	s.owned = 0
	s.activeNativeHistogram = 0
}

func (s *activeSeriesStripe) getActive() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.active
}

func (s *activeSeriesStripe) getOwned() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.owned
}

func (s *activeSeriesStripe) getActiveNativeHistogram() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.activeNativeHistogram
}

// isOwned reports whether the series with the given ring token is owned by this
// instance, meaning this instance is one of the replicas the ring selects for it.
//
// The answer is a binary search over the ring's token list followed by an array
// index into a bitmap precomputed once per ring change, so this is cheap enough
// to call on the push path.
//
// When ownership is unknown, because the ring has not been read yet or the bitmap
// does not match the token list, this returns true. Ownership is used to decide
// what counts against a limit, so the safe direction is to assume the series is
// ours: over-counting delays an unrelated scale-up, while under-counting would
// let a tenant exceed its limit.
func isOwned(key uint32, tokens []uint32, ownedPositions []bool) bool {
	if len(tokens) == 0 || len(ownedPositions) != len(tokens) {
		return true
	}

	return ownedPositions[ring.SearchToken(tokens, key)]
}

// matchesAll returns true if the labels satisfy all given matchers.
func matchesAll(lbs labels.Labels, matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		if !m.Matches(lbs.Get(m.Name)) {
			return false
		}
	}
	return true
}
