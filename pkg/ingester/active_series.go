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
//
// It maintains two independent counts over the same set of entries:
//
//   - active: entries whose last sample is newer than the idle timeout. This is
//     the long-standing cortex_ingester_active_series gauge and its value is
//     unchanged by owned-series tracking.
//   - owned: entries whose ring token places them on this ingester, regardless of
//     how recently they received a sample.
//
// The two use different retention, and that is deliberate. Entries live until
// Purge is called with the TSDB head's minimum time, so owned counts everything
// still pinned in the head. That makes owned a proxy for the memory the tenant
// is actually holding here, which is what a series limit is protecting. Because
// owned ignores the idle window while active applies it, owned may exceed active
// for a tenant with high churn.
type ActiveSeries struct {
	// Ring ownership state. Readers on the push path load atomically;
	// the writer (updateTokens) stores a new pointer on ring changes.
	ring atomic.Pointer[ringState]

	// currFingerprint detects ring changes. It is the fingerprint reported by the
	// lifecycler alongside the ownership bitmap, which changes if and only if
	// ownership may have changed. Only accessed by the updateTokens caller
	// (periodic updateActiveSeries goroutine), so no synchronization needed.
	currFingerprint uint64

	// activeCutoffNanos is the idle cutoff used by the most recent UpdateMetrics,
	// published so that the push path can tell when a retained entry crosses back
	// into the active window. Zero means no cycle has run with a cutoff yet, which
	// is the case when entries are released by the idle timeout instead of being
	// retained, and then a returning series simply creates a new entry.
	activeCutoffNanos uatomic.Int64

	stripes [numActiveSeriesStripes]activeSeriesStripe
}

// activeSeriesStripe holds a subset of the series timestamps for a single tenant.
type activeSeriesStripe struct {
	// Unix nanoseconds. Only used by purge. Zero = unknown.
	// Updated in purge and when old timestamp is used when updating series (in this case, oldestEntryTs is updated
	// without holding the lock -- hence the atomic).
	oldestEntryTs uatomic.Int64

	mu   sync.RWMutex
	refs map[uint64][]activeSeriesEntry

	// Counters for this stripe. Every mutation happens while holding mu, so they
	// cannot be lost, but they are atomics so that readers can total them across
	// stripes without acquiring 512 locks.
	active                uatomic.Int64 // Entries in this stripe within the idle window.
	activeNativeHistogram uatomic.Int64 // Native histogram entries in this stripe within the idle window.
	owned                 uatomic.Int64 // Entries in this stripe owned by this instance, ignoring the idle window.
}

// activeSeriesEntry holds a timestamp for single series.
type activeSeriesEntry struct {
	lbs labels.Labels
	key uint32 // Ring token hash for this series (used for ownership checks)
	// owned caches whether key belongs to this instance, so that counting owned
	// series does not repeat the ring lookup on every pass. Refreshed only when
	// the ring changes. Guarded by the stripe mutex.
	owned             bool
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
// for this series (computed via ring.TokenForLabels). When the ring is not loaded,
// ownership is unknown and the series counts as owned.
//
// Every series is tracked. Ownership only decides whether the series counts
// towards owned, never whether it is tracked at all, so the active count behaves
// exactly as it did before owned-series tracking existed.
func (c *ActiveSeries) UpdateSeries(series labels.Labels, hash uint64, key uint32, now time.Time, nativeHistogram bool, labelsCopy func(labels.Labels) labels.Labels) {
	stripeID := hash % numActiveSeriesStripes

	// Load ring state atomically — readers on the push path always see a consistent snapshot.
	state := c.ring.Load()
	c.stripes[stripeID].updateSeriesTimestamp(
		now, series, hash, key, nativeHistogram, labelsCopy, state.tokens, state.ownedPositions, c.activeCutoffNanos.Load())
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

// UpdateMetrics recomputes the active and owned counts, re-evaluating ownership if
// the ring changed. Called from updateActiveSeries when OwnedMetrics is enabled.
//
// This deliberately removes nothing. Entries are released by Purge at head
// compaction instead, so that owned keeps counting series which are still in the
// head but have gone idle. keepUntil therefore only decides which entries count
// as active.
func (c *ActiveSeries) UpdateMetrics(keepUntil time.Time, tokens []uint32, ownedPositions []bool, fingerprint uint64) {
	tokensChanged := c.updateTokens(tokens, ownedPositions, fingerprint)

	// Load the ring state from the atomic pointer for consistency.
	// Even though we're on the same goroutine that just stored it, reading from
	// the pointer ensures all code paths use the same access pattern.
	state := c.ring.Load()

	for s := range numActiveSeriesStripes {
		c.stripes[s].updateMetrics(keepUntil, tokensChanged, state.tokens, state.ownedPositions)
	}

	// Publish the cutoff after the recount, so the push path compares against a
	// cutoff the stripe counters have already been brought in line with.
	c.activeCutoffNanos.Store(keepUntil.UnixNano())
}

// Purge removes entries last updated before deleteBefore and recomputes the counts.
//
// The two cutoffs are separate because they answer different questions.
// deleteBefore decides what is still held: with owned-series tracking enabled it
// is derived from the TSDB head's minimum time, so an entry survives as long as
// the series it describes is in the head. activeCutoff decides what counts as
// active, and is always the idle timeout.
//
// Conflating them would redefine the active count. Passing the head cutoff as both
// would count every retained entry as active, including ones idle for hours, which
// is the owned count rather than the active one.
//
// When tracking is disabled both are the idle timeout, which is the original
// behaviour.
func (c *ActiveSeries) Purge(deleteBefore, activeCutoff time.Time) {
	for s := range numActiveSeriesStripes {
		c.stripes[s].purge(deleteBefore, activeCutoff)
	}
}

// clear drops every tracked entry. Used when the TSDB head is empty, so that no
// entry outlives the series it describes.
func (c *ActiveSeries) clear() {
	for s := range numActiveSeriesStripes {
		c.stripes[s].clear()
	}
}

// Active returns the number of series which received a sample more recently than
// the idle timeout. Its value is not affected by ownership tracking.
func (c *ActiveSeries) Active() int {
	total := int64(0)
	for s := range numActiveSeriesStripes {
		total += c.stripes[s].active.Load()
	}
	return int(total)
}

// Owned returns the number of tracked series whose ring token places them on this
// instance, ignoring the idle window.
//
// This may exceed Active for a tenant with high churn, because it also counts
// idle series which are still held in the TSDB head. That is intended: it is the
// count of series this instance is actually storing, which is what the series
// limit exists to bound.
//
// Before the ring has been read, ownership is unknown and every series counts as
// owned, so this equals Active.
func (c *ActiveSeries) Owned() int {
	total := int64(0)
	for s := range numActiveSeriesStripes {
		total += c.stripes[s].owned.Load()
	}
	return int(total)
}

func (c *ActiveSeries) ActiveNativeHistogram() int {
	total := int64(0)
	for s := range numActiveSeriesStripes {
		total += c.stripes[s].activeNativeHistogram.Load()
	}
	return int(total)
}

// updateSeriesTimestamp records a sample for a series, creating the entry if this
// is the first time it has been seen. It reports whether an entry was created and,
// if so, whether that entry is owned by this instance.
func (s *activeSeriesStripe) updateSeriesTimestamp(now time.Time, series labels.Labels, fingerprint uint64, key uint32, nativeHistogram bool, labelsCopy func(labels.Labels) labels.Labels, tokens []uint32, ownedPositions []bool, activeCutoffNanos int64) {
	nowNanos := now.UnixNano()

	e := s.findEntryForSeries(fingerprint, series)
	entryTimeSet := false
	if e == nil {
		e, entryTimeSet = s.findOrCreateEntryForSeries(fingerprint, key, series, nowNanos, nativeHistogram, labelsCopy, tokens, ownedPositions)
	}

	if !entryTimeSet {
		if prev := e.Load(); nowNanos > prev {
			if entryTimeSet = e.CompareAndSwap(prev, nowNanos); entryTimeSet &&
				activeCutoffNanos > 0 && prev < activeCutoffNanos && nowNanos >= activeCutoffNanos {
				// This entry has just crossed back into the active window. Only the
				// goroutine whose compare-and-swap moved the timestamp across the
				// cutoff gets here, so the series is counted exactly once.
				s.countReactivation(fingerprint, series)
			}
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

// countReactivation records that a retained entry has re-entered the active window.
// It reports whether the entry was found, and whether it is a native histogram, which
// is taken from the entry rather than from the incoming sample because the entry's
// kind is fixed when it is created.
func (s *activeSeriesStripe) countReactivation(fingerprint uint64, series labels.Labels) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, entry := range s.refs[fingerprint] {
		if labels.Equal(entry.lbs, series) {
			s.active.Inc()
			if entry.isNativeHistogram {
				s.activeNativeHistogram.Inc()
			}
			return
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

func (s *activeSeriesStripe) findOrCreateEntryForSeries(fingerprint uint64, key uint32, series labels.Labels, nowNanos int64, nativeHistogram bool, labelsCopy func(labels.Labels) labels.Labels, tokens []uint32, ownedPositions []bool) (nanos *uatomic.Int64, entryTimeSet bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if already exists within the entries.
	for ix, entry := range s.refs[fingerprint] {
		if labels.Equal(entry.lbs, series) {
			return s.refs[fingerprint][ix].nanos, false
		}
	}

	// Ownership decides which counters this series contributes to, not whether it
	// is tracked. A series this instance does not own is still an active series.
	owned := isOwned(key, tokens, ownedPositions)

	s.active.Inc()
	if owned {
		s.owned.Inc()
	}
	if nativeHistogram {
		s.activeNativeHistogram.Inc()
	}

	e := activeSeriesEntry{
		lbs:               labelsCopy(series),
		key:               key,
		owned:             owned,
		nanos:             uatomic.NewInt64(nowNanos),
		isNativeHistogram: nativeHistogram,
	}

	s.refs[fingerprint] = append(s.refs[fingerprint], e)

	return e.nanos, true
}

// updateMetrics recounts this stripe's active and owned series, refreshing each
// entry's cached ownership if the ring changed.
//
// It removes nothing: releasing entries is Purge's job, and keeping idle entries
// is what allows owned to track what is in the head rather than what is in the
// idle window.
//
// There is no shortcut for "nothing expired" the way purge has, because entries
// leave the active window silently now that they are not deleted, so the counts
// have to be recomputed. The scan is the same order of work the purge it replaces
// performed on a tenant that was churning.
func (s *activeSeriesStripe) updateMetrics(keepUntil time.Time, tokensChanged bool, tokens []uint32, ownedPositions []bool) {
	var active, owned, activeNativeHistogram int
	keepUntilNanos := keepUntil.UnixNano()

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, entries := range s.refs {
		for i := range entries {
			if tokensChanged {
				entries[i].owned = isOwned(entries[i].key, tokens, ownedPositions)
			}

			if entries[i].owned {
				owned++
			}

			if entries[i].nanos.Load() >= keepUntilNanos {
				active++
				if entries[i].isNativeHistogram {
					activeNativeHistogram++
				}
			}
		}
	}

	s.active.Store(int64(active))
	s.owned.Store(int64(owned))
	s.activeNativeHistogram.Store(int64(activeNativeHistogram))
}

// purge removes entries last updated before keepUntil and returns the resulting
// counts for this stripe.
func (s *activeSeriesStripe) purge(deleteBefore, activeCutoff time.Time) {
	var active, owned, activeNativeHistogram int
	deleteBeforeNanos := deleteBefore.UnixNano()
	activeCutoffNanos := activeCutoff.UnixNano()
	if oldest := s.oldestEntryTs.Load(); oldest > 0 && deleteBeforeNanos <= oldest {
		// Nothing to remove, so the counts cannot have changed.
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	oldest := int64(math.MaxInt64)
	for fp, entries := range s.refs {
		if len(entries) == 1 {
			ts := entries[0].nanos.Load()
			if ts < deleteBeforeNanos {
				delete(s.refs, fp)
				continue
			}

			// Retained. Owned ignores the idle window; active applies it.
			if entries[0].owned {
				owned++
			}
			if ts >= activeCutoffNanos {
				active++
				if entries[0].isNativeHistogram {
					activeNativeHistogram++
				}
			}
			if ts < oldest {
				oldest = ts
			}
			continue
		}

		for i := 0; i < len(entries); {
			ts := entries[i].nanos.Load()
			if ts < deleteBeforeNanos {
				entries = append(entries[:i], entries[i+1:]...)
			} else {
				if ts < oldest {
					oldest = ts
				}
				if entries[i].owned {
					owned++
				}
				if ts >= activeCutoffNanos {
					active++
					if entries[i].isNativeHistogram {
						activeNativeHistogram++
					}
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
	s.active.Store(int64(active))
	s.owned.Store(int64(owned))
	s.activeNativeHistogram.Store(int64(activeNativeHistogram))
}

// nolint // Linter reports that this method is unused, but it is.
func (s *activeSeriesStripe) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.oldestEntryTs.Store(0)
	s.refs = map[uint64][]activeSeriesEntry{}
	s.active.Store(0)
	s.owned.Store(0)
	s.activeNativeHistogram.Store(0)
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
