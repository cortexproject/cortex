package ingester

import (
	"math"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	uatomic "go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ring"
)

const (
	numActiveSeriesStripes = 512
)

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
// The two use different retention, and that is deliberate. With owned-series
// tracking, entries live until the head deletes their series, which releases
// them through DeleteSeries, so owned counts everything still held in the head. That makes owned a proxy for the memory the tenant
// is actually holding here, which is what a series limit is protecting. Because
// owned ignores the idle window while active applies it, owned may exceed active
// for a tenant with high churn.
type ActiveSeries struct {
	// Ownership state, which the recount evaluates entries against: the sorted
	// tokens of the ring the tenant's writes are routed within, and a parallel
	// bitmap of the positions this ingester owns. Both come from
	// ring.Ring.OwnedTokenPositions and are never modified.
	//
	// currFingerprint identifies the ownership, and changes if and only if it
	// does. Zero means none has been applied yet, and every entry counts as owned.
	//
	// viewRead is the number of the lifecycler's ring read the ownership was last
	// applied as of. An entry created at or after that read may have been routed
	// here by a newer ring than the one ownership was computed from, so it is not
	// judged by it. See lifecycler RingDesc.
	//
	// All of these are only accessed from the ingester's owned-series loop, by
	// UpdateMetrics and the recount deciding whether to call it, so no
	// synchronization is needed.
	tokens          []uint32
	ownedPositions  []bool
	currFingerprint uint64
	viewRead        uint32

	// activeCutoffNanos is the idle cutoff used by the most recent UpdateMetrics,
	// published so that the push path can tell when a retained entry crosses back
	// into the active window. Zero means no cycle has run with a cutoff yet, which
	// is the case when entries are released by the idle timeout instead of being
	// retained, and then a returning series simply creates a new entry.
	activeCutoffNanos uatomic.Int64

	// totals holds each stripe counter totalled across every stripe. See
	// activeSeriesTotals.
	totals activeSeriesTotals

	stripes [numActiveSeriesStripes]activeSeriesStripe
}

// activeSeriesTotals holds each of the per-stripe counters totalled across every
// stripe, so that reading one does not have to add up 512 stripes.
//
// The per-tenant limit check reads two of these for every new series, and adding
// up the stripes was the largest single cost on that path: about 630ns, more than
// the rest of creating a series put together.
//
// Every mutation of a stripe counter applies the same delta here, while holding
// that stripe's lock, so a total stays equal to the sum of the stripes rather
// than being recomputed from a scan. An earlier revision of this file cached the
// totals and had the periodic recount store an absolute sum computed from its
// scan, which clobbered any increment made after the scan had read a stripe and
// before the store landed. Applying deltas cannot lose an update: a stripe's own
// mutations are serialised by its lock, and deltas from different stripes
// commute, so they do not need to be ordered against each other.
//
// A reader can still observe a total between a stripe's update and the matching
// delta, so it is off by at most that delta for the few instructions in between.
// That is inherent to counters maintained without a global lock, and it is no
// weaker than totalling 512 atomics, which cannot produce a consistent snapshot
// across stripes either.
//
// The four counters deliberately share a cache line. They are mutated together by
// whichever goroutine creates or releases an entry, so one line changing hands
// costs less than four would.
type activeSeriesTotals struct {
	tracked               uatomic.Int64
	active                uatomic.Int64
	activeNativeHistogram uatomic.Int64
	owned                 uatomic.Int64
}

// activeSeriesStripe holds a subset of the series timestamps for a single tenant.
type activeSeriesStripe struct {
	// Unix nanoseconds. Only used by purge. Zero = unknown.
	// Updated in purge and when old timestamp is used when updating series (in this case, oldestEntryTs is updated
	// without holding the lock -- hence the atomic).
	oldestEntryTs uatomic.Int64

	mu   sync.RWMutex
	refs map[uint64][]activeSeriesEntry

	// totals is the tracker's cross-stripe totals. Every mutation of the counters
	// below applies its delta there too, through the helpers, so that no reader
	// has to visit the stripes. Set by NewActiveSeries and never nil.
	totals *activeSeriesTotals

	// Counters for this stripe. Every mutation happens while holding mu, so they
	// cannot be lost, but they are atomics so that a reader can total them across
	// stripes without acquiring 512 locks, which is how tests check them against
	// the cross-stripe totals.
	tracked               uatomic.Int64 // All entries in this stripe, regardless of age or ownership.
	active                uatomic.Int64 // Entries in this stripe within the idle window.
	activeNativeHistogram uatomic.Int64 // Native histogram entries in this stripe within the idle window.
	owned                 uatomic.Int64 // Entries in this stripe owned by this instance, ignoring the idle window.
}

// activeSeriesEntry holds a timestamp for single series.
type activeSeriesEntry struct {
	lbs labels.Labels
	key uint32 // Ring token hash for this series (used for ownership checks)
	// createdRead is the lifecycler's ring read number when the series arrived.
	// The series is only judged by a view of the ring at least that new.
	createdRead uint32
	// owned caches whether key belongs to this instance, so that counting owned
	// series does not repeat the ring lookup on every pass. A new entry is owned,
	// and ownership is re-evaluated only as UpdateMetrics describes. Guarded by
	// the stripe mutex.
	owned bool
	// keyless marks an entry whose ring token could not be computed. Its
	// ownership cannot be evaluated, so it always counts as owned.
	keyless           bool
	isNativeHistogram bool
	// seeded marks an entry created from the TSDB head after a restart rather
	// than by a sample. It has not received a sample since, so it does not count
	// as active, and its kind is unknown until its first sample sets it.
	seeded bool
	nanos  *uatomic.Int64 // Unix timestamp in nanoseconds. Needs to be a pointer because we don't store pointers to entries in the stripe.
	// headRef is the TSDB head series reference this entry describes, refreshed
	// on every sample. Head series references are never reused within a TSDB, so
	// it identifies the exact head series: when the head deletes a series, only
	// the entry still carrying that reference is removed. It is nil when the
	// caller supplies no reference, as with owned-series tracking disabled, so
	// that path allocates nothing extra; such entries are never removed by
	// DeleteSeries.
	headRef *uatomic.Uint64
}

func NewActiveSeries() *ActiveSeries {
	c := &ActiveSeries{}

	// Stripes are pre-allocated so that we only read on them and no lock is required.
	for i := range numActiveSeriesStripes {
		c.stripes[i].refs = map[uint64][]activeSeriesEntry{}
		c.stripes[i].totals = &c.totals
	}

	return c
}

// addTracked, addActive, addOwned and storeCounts are the only places the stripe
// counters are written, so that the cross-stripe totals can never be left behind
// by a new call site. Each must be called while holding the stripe's lock.
func (s *activeSeriesStripe) addTracked(delta int64) {
	s.tracked.Add(delta)
	s.totals.tracked.Add(delta)
}

// addActive moves the active count, and the native histogram count with it when
// the entry is one, since every entry counted as a native histogram is also
// counted as active.
func (s *activeSeriesStripe) addActive(delta int64, nativeHistogram bool) {
	s.active.Add(delta)
	s.totals.active.Add(delta)
	if nativeHistogram {
		s.activeNativeHistogram.Add(delta)
		s.totals.activeNativeHistogram.Add(delta)
	}
}

func (s *activeSeriesStripe) addOwned(delta int64) {
	s.owned.Add(delta)
	s.totals.owned.Add(delta)
}

// storeCounts replaces this stripe's counters with the result of a full recount
// of it, applying the difference to the totals. The difference comes from a swap
// rather than a separate load, so that the delta is exactly what this stripe
// moved by even though the counters are read and written separately.
func (s *activeSeriesStripe) storeCounts(tracked, active, activeNativeHistogram, owned int64) {
	s.totals.tracked.Add(tracked - s.tracked.Swap(tracked))
	s.totals.active.Add(active - s.active.Swap(active))
	s.totals.activeNativeHistogram.Add(activeNativeHistogram - s.activeNativeHistogram.Swap(activeNativeHistogram))
	s.totals.owned.Add(owned - s.owned.Swap(owned))
}

// seriesToken describes a series' ring token, the hash the distributor routes it
// by, so that the token is computed only for a series seen for the first time:
// the tracker needs it to create the series' entry, and never again for that
// series. It is a value type, so passing it on every sample allocates nothing.
type seriesToken struct {
	// key is the token, if the caller already knows it.
	key   uint32
	known bool

	// Otherwise the token is computed from these, as the distributor does. A
	// zero value, with no labels, has no token.
	userID           string
	labels           []cortexpb.LabelAdapter
	shardByAllLabels bool

	// ringRead is the lifecycler's ring read number when the sample arrived. See
	// activeSeriesEntry.createdRead.
	ringRead uint32
}

// knownSeriesToken returns a seriesToken for a token the caller already knows.
func knownSeriesToken(key uint32) seriesToken {
	return seriesToken{key: key, known: true}
}

// resolve returns the token, and false if there is none.
func (t seriesToken) resolve() (uint32, bool) {
	if t.known {
		return t.key, true
	}
	if t.labels == nil {
		return 0, false
	}
	key, err := ring.TokenForLabels(t.userID, t.labels, t.shardByAllLabels)
	if err != nil {
		return 0, false
	}
	return key, true
}

// UpdateSeries updates series timestamp to 'now'. The key parameter is the ring token
// for this series (computed via ring.TokenForLabels).
//
// Every series is tracked. Ownership only decides whether the series counts
// towards owned, never whether it is tracked at all, so the active count behaves
// exactly as it did before owned-series tracking existed.
func (c *ActiveSeries) UpdateSeries(series labels.Labels, hash uint64, key uint32, now time.Time, nativeHistogram bool, labelsCopy func(labels.Labels) labels.Labels) {
	token := knownSeriesToken(key)
	c.stripes[hash%numActiveSeriesStripes].updateSeriesTimestamp(now, series, hash, 0, &token, nativeHistogram, labelsCopy, &c.activeCutoffNanos)
}

// UpdateSeriesWithRef is UpdateSeries for callers that know the TSDB head series
// reference the sample was appended to, and whose ring token is computed only if
// the series is new. Recording the reference lets DeleteSeries release the entry
// exactly when the head drops that series.
func (c *ActiveSeries) UpdateSeriesWithRef(series labels.Labels, hash uint64, headRef uint64, token seriesToken, now time.Time, nativeHistogram bool, labelsCopy func(labels.Labels) labels.Labels) {
	c.stripes[hash%numActiveSeriesStripes].updateSeriesTimestamp(now, series, hash, headRef, &token, nativeHistogram, labelsCopy, &c.activeCutoffNanos)
}

// SeedSeries tracks a series which is in the TSDB head but has no entry, as after
// a restart, when the head is rebuilt from the WAL without going through Push.
// Without an entry such a series counts as owned until head compaction drops it,
// even if the ring now sends it elsewhere, which after a rolling restart that
// follows a scale-up would throttle tenants as if owned series were not tracked.
//
// The entry counts as owned until a view of the ring read after token.ringRead
// judges it, like a new series. It is not active, since it has received no sample
// since the restart: nanos should be outside the active window. Its first sample
// makes it active and sets whether it is a native histogram.
func (c *ActiveSeries) SeedSeries(series labels.Labels, hash uint64, headRef uint64, token seriesToken, nanos int64) {
	key, keyed := token.resolve()
	c.stripes[hash%numActiveSeriesStripes].seed(hash, headRef, key, keyed, token.ringRead, series, nanos)
}

// DeleteSeries releases the entries for series the TSDB head has just deleted. It
// is driven by the head's PostDeletion callback, so an entry lives exactly as long
// as the series it describes is in memory, with no clock comparison involved.
//
// An entry is removed only if it still carries the deleted series' reference. The
// head invokes the callback after releasing its own lock, so a sample can re-create
// the same labels as a new head series before this runs. That sample refreshes the
// entry to the new reference, and the entry is then correctly kept.
func (c *ActiveSeries) DeleteSeries(deleted map[chunks.HeadSeriesRef]labels.Labels) {
	activeCutoffNanos := c.activeCutoffNanos.Load()
	for ref, lbls := range deleted {
		if ref == 0 {
			continue
		}
		fp := lbls.Hash()
		c.stripes[fp%numActiveSeriesStripes].deleteSeries(fp, lbls, uint64(ref), activeCutoffNanos)
	}
}

// updateTokens records the ownership to evaluate entries against, and reports
// whether it changed. A fingerprint equal to the current one means the same
// ownership, possibly in new slices from a rebuilt ring, which are adopted so
// that the previous ring's can be freed.
func (c *ActiveSeries) updateTokens(tokens []uint32, ownedPositions []bool, fingerprint uint64) bool {
	if len(tokens) == 0 || fingerprint == 0 {
		return false
	}
	c.tokens, c.ownedPositions = tokens, ownedPositions
	if fingerprint == c.currFingerprint {
		return false
	}
	c.currFingerprint = fingerprint
	return true
}

// UpdateMetrics recomputes the active and owned counts, evaluating entries'
// ownership when it may have changed. Called from the ingester's update loop when
// owned series are tracked.
//
// viewRead is the number of the lifecycler's ring read the ownership is up to
// date as of. Every entry is evaluated when the ownership changed. When only
// viewRead advanced, the entries too new to be judged by the previous view are.
// An entry is never judged by a view older than itself: it may have been routed
// here by a newer ring than this ingester has seen, so it stays owned until a
// view at least as new as the entry arrives.
//
// This deliberately removes nothing. Entries are released by DeleteSeries when
// the head deletes their series, so that owned keeps counting series which are
// still in the head but have gone idle. keepUntil therefore only decides which
// entries count as active.
func (c *ActiveSeries) UpdateMetrics(keepUntil time.Time, tokens []uint32, ownedPositions []bool, fingerprint uint64, viewRead uint32) {
	changed := c.updateTokens(tokens, ownedPositions, fingerprint)

	var j judgement
	switch {
	case c.currFingerprint == 0, fingerprint == 0:
		// No ownership yet, or none known for this view: nothing is judged, and
		// the view is not recorded, since nothing was computed from it.
	case changed:
		j = judgement{all: true, viewRead: viewRead, tokens: c.tokens, ownedPositions: c.ownedPositions}
		c.viewRead = viewRead
	case viewRead > c.viewRead:
		// The same ownership, seen by a newer read. Judge the entries the
		// previous view was too new for.
		j = judgement{pending: true, pendingFrom: c.viewRead, viewRead: viewRead, tokens: c.tokens, ownedPositions: c.ownedPositions}
		c.viewRead = viewRead
	}

	for s := range numActiveSeriesStripes {
		c.stripes[s].updateMetrics(keepUntil, j)
	}

	// Publish the cutoff after the recount, so the push path compares against a
	// cutoff the stripe counters have already been brought in line with.
	c.activeCutoffNanos.Store(keepUntil.UnixNano())
}

// Purge removes entries last updated before deleteBefore and recomputes the counts.
//
// The two cutoffs are separate because they answer different questions.
// deleteBefore decides what is still held: with owned-series tracking enabled it
// is set well past anything the head can still hold, as a backstop for entries
// DeleteSeries did not release. activeCutoff decides what counts as active, and
// is always the idle timeout.
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

// activeCutoff returns the idle cutoff of the last recount, which the push and
// deletion paths count active series against, or the zero time before any.
func (c *ActiveSeries) activeCutoff() time.Time {
	return time.Unix(0, c.activeCutoffNanos.Load())
}

// Active returns the number of series which received a sample more recently than
// the idle timeout. Its value is not affected by ownership tracking.
func (c *ActiveSeries) Active() int {
	return int(c.totals.active.Load())
}

// Owned returns the number of tracked series whose ring token places them on this
// instance, ignoring the idle window.
//
// This may exceed Active for a tenant with high churn, because it also counts
// idle series which are still held in the TSDB head. That is intended: it is the
// count of series this instance is actually storing, which is what the series
// limit exists to bound.
//
// Before any ownership has been applied every series counts as owned, so this
// equals Tracked.
func (c *ActiveSeries) Owned() int {
	return int(c.totals.owned.Load())
}

// Tracked returns the number of series this tracker holds an entry for, whatever
// their age or ownership.
//
// Entries are only created when a sample arrives, so after a restart this is zero
// while the TSDB head is already full of series restored from the WAL. Comparing it
// against the head's series count is therefore how a caller tells whether the owned
// count covers what is actually in memory, or only the part of it seen since start.
func (c *ActiveSeries) Tracked() int {
	return int(c.totals.tracked.Load())
}

func (c *ActiveSeries) ActiveNativeHistogram() int {
	return int(c.totals.activeNativeHistogram.Load())
}

// updateSeriesTimestamp records a sample for a series, creating the entry if this
// is the first time it has been seen.
//
// The token and the active cutoff are passed by reference because the update of
// an existing series, which is the common case, needs neither until its
// timestamp has moved, and this runs for every sample.
func (s *activeSeriesStripe) updateSeriesTimestamp(now time.Time, series labels.Labels, fingerprint uint64, headRef uint64, token *seriesToken, nativeHistogram bool, labelsCopy func(labels.Labels) labels.Labels, activeCutoff *uatomic.Int64) {
	nowNanos := now.UnixNano()

	e, ref, seeded := s.findEntryForSeries(fingerprint, series)
	entryTimeSet := false
	if e == nil {
		// Only a series seen for the first time needs its token, and it is
		// computed before taking the stripe lock, so as not to hold it longer.
		key, keyed := token.resolve()
		e, ref, entryTimeSet = s.findOrCreateEntryForSeries(fingerprint, headRef, key, keyed, token.ringRead, series, nowNanos, nativeHistogram, labelsCopy)
	}

	// Keep the entry pointing at the live head series. It changes only when the
	// head deleted the series and a later sample re-created it.
	if headRef != 0 {
		if ref == nil {
			// The entry was created without a reference; adopt this one so the
			// head's deletion callback can release it.
			s.setHeadRef(fingerprint, series, headRef)
		} else if ref.Load() != headRef {
			ref.Store(headRef)
		}
	}

	if !entryTimeSet {
		if prev := e.Load(); nowNanos > prev {
			entryTimeSet = e.CompareAndSwap(prev, nowNanos)
			switch {
			case !entryTimeSet:
			case seeded:
				// The first sample of a series seeded from the head.
				s.settleSeeded(fingerprint, series, nativeHistogram)
			case crossedCutoff(prev, nowNanos, activeCutoff.Load()):
				// This entry has just crossed back into the active window. Only the
				// goroutine whose compare-and-swap moved the timestamp across the
				// cutoff gets here, so the series is counted exactly once.
				s.countReactivation(fingerprint, series, nativeHistogram)
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

// crossedCutoff reports whether a timestamp moving from prev to now has crossed
// back into the active window. A zero cutoff means none has been published.
func crossedCutoff(prev, now, cutoff int64) bool {
	return cutoff > 0 && prev < cutoff && now >= cutoff
}

// countReactivation records that a retained entry has re-entered the active window.
// The entry takes the kind of the sample that reactivated it. Without retention
// the entry would have been released when it went idle, and re-created by this
// sample with this sample's kind, so a series which switched between float and
// native histogram samples counts as what it is now.
func (s *activeSeriesStripe) countReactivation(fingerprint uint64, series labels.Labels, nativeHistogram bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range s.refs[fingerprint] {
		e := &s.refs[fingerprint][i]
		if labels.Equal(e.lbs, series) {
			e.isNativeHistogram = nativeHistogram
			s.addActive(1, nativeHistogram)
			return
		}
	}
}

// settleSeeded gives a series seeded from the head its kind, from its first
// sample, and counts it as active. Only the first call for an entry does anything.
func (s *activeSeriesStripe) settleSeeded(fingerprint uint64, series labels.Labels, nativeHistogram bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range s.refs[fingerprint] {
		e := &s.refs[fingerprint][i]
		if !labels.Equal(e.lbs, series) {
			continue
		}
		if !e.seeded {
			return
		}
		e.seeded = false
		e.isNativeHistogram = nativeHistogram
		s.addActive(1, nativeHistogram)
		return
	}
}

// seed creates an entry for a series in the head which the tracker has not seen,
// as owned, not active, and of unknown kind. See ActiveSeries.SeedSeries.
func (s *activeSeriesStripe) seed(fingerprint uint64, headRef uint64, key uint32, keyed bool, createdRead uint32, series labels.Labels, nanos int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, entry := range s.refs[fingerprint] {
		if labels.Equal(entry.lbs, series) {
			return
		}
	}

	s.addTracked(1)
	s.addOwned(1)
	e := activeSeriesEntry{
		lbs:         series,
		key:         key,
		createdRead: createdRead,
		owned:       true,
		keyless:     !keyed,
		seeded:      true,
		nanos:       uatomic.NewInt64(nanos),
	}
	if headRef != 0 {
		e.headRef = uatomic.NewUint64(headRef)
	}
	s.refs[fingerprint] = append(s.refs[fingerprint], e)

	for prevOldest := s.oldestEntryTs.Load(); nanos < prevOldest; {
		if s.oldestEntryTs.CompareAndSwap(prevOldest, 0) {
			break
		}
	}
}

func (s *activeSeriesStripe) findEntryForSeries(fingerprint uint64, series labels.Labels) (*uatomic.Int64, *uatomic.Uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check if already exists within the entries. Index rather than range by
	// value, so as not to copy each entry: this runs for every sample.
	entries := s.refs[fingerprint]
	for ix := range entries {
		if e := &entries[ix]; labels.Equal(e.lbs, series) {
			return e.nanos, e.headRef, e.seeded
		}
	}

	return nil, nil, false
}

func (s *activeSeriesStripe) findOrCreateEntryForSeries(fingerprint uint64, headRef uint64, key uint32, keyed bool, createdRead uint32, series labels.Labels, nowNanos int64, nativeHistogram bool, labelsCopy func(labels.Labels) labels.Labels) (nanos *uatomic.Int64, ref *uatomic.Uint64, entryTimeSet bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if already exists within the entries.
	for ix, entry := range s.refs[fingerprint] {
		if labels.Equal(entry.lbs, series) {
			return s.refs[fingerprint][ix].nanos, s.refs[fingerprint][ix].headRef, false
		}
	}

	// A new series is owned. It reached this instance because the distributor
	// routed it here, which is the question ownership answers. This instance's
	// own view of the ring is not consulted, because it can lag the
	// distributor's: if this instance has just gained token ranges and not yet
	// seen the ring change, judging the series by its stale view would count it
	// as owned by nobody, and let it past the limits. Ownership is re-evaluated
	// against the ring, along with every other entry's, when the ring changes.
	//
	// Ownership decides which counters a series contributes to, not whether it is
	// tracked. A series this instance does not own is still an active series.
	owned := true

	s.addTracked(1)
	s.addActive(1, nativeHistogram)
	s.addOwned(1)

	e := activeSeriesEntry{
		lbs:               labelsCopy(series),
		key:               key,
		createdRead:       createdRead,
		owned:             owned,
		keyless:           !keyed,
		nanos:             uatomic.NewInt64(nowNanos),
		isNativeHistogram: nativeHistogram,
	}
	if headRef != 0 {
		e.headRef = uatomic.NewUint64(headRef)
	}

	s.refs[fingerprint] = append(s.refs[fingerprint], e)

	return e.nanos, e.headRef, true
}

// setHeadRef gives an existing entry that has no head reference the given one.
func (s *activeSeriesStripe) setHeadRef(fingerprint uint64, series labels.Labels, headRef uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range s.refs[fingerprint] {
		e := &s.refs[fingerprint][i]
		if !labels.Equal(e.lbs, series) {
			continue
		}
		if e.headRef == nil {
			e.headRef = uatomic.NewUint64(headRef)
		} else {
			e.headRef.Store(headRef)
		}
		return
	}
}

// deleteSeries removes the entry for series if it still refers to headRef, and
// takes it out of every count it contributed to.
func (s *activeSeriesStripe) deleteSeries(fingerprint uint64, series labels.Labels, headRef uint64, activeCutoffNanos int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries := s.refs[fingerprint]
	for i := range entries {
		if !labels.Equal(entries[i].lbs, series) {
			continue
		}
		if entries[i].headRef == nil || entries[i].headRef.Load() != headRef {
			// Re-created as a new head series since the head deleted this one.
			return
		}

		e := entries[i]
		s.addTracked(-1)
		if e.owned {
			s.addOwned(-1)
		}
		// Mirror how the entry was counted: everything counts as active until the
		// first recount publishes a cutoff, and after that only entries inside it.
		if !e.seeded && (activeCutoffNanos == 0 || e.nanos.Load() >= activeCutoffNanos) {
			s.addActive(-1, e.isNativeHistogram)
		}

		entries = append(entries[:i], entries[i+1:]...)
		if len(entries) == 0 {
			delete(s.refs, fingerprint)
		} else {
			s.refs[fingerprint] = entries
		}
		return
	}
}

// judgement says which entries a recount evaluates the ownership of, and how.
type judgement struct {
	// all evaluates every entry.
	all bool
	// pending evaluates the entries created at or after the pendingFrom read.
	pending     bool
	pendingFrom uint32

	viewRead       uint32
	tokens         []uint32
	ownedPositions []bool
}

func (j judgement) owned(e *activeSeriesEntry) bool {
	if e.keyless || e.createdRead >= j.viewRead {
		// Not judged: no token, or newer than the view of the ring.
		return true
	}
	return isOwned(e.key, j.tokens, j.ownedPositions)
}

// updateMetrics recounts this stripe's active and owned series, refreshing each
// entry's cached ownership if the ring changed.
//
// It removes nothing: entries are released by deleteSeries when the head deletes
// their series, and keeping idle entries is what allows owned to track what is in
// the head rather than what is in the idle window.
//
// There is no shortcut for "nothing expired" the way purge has, because entries
// leave the active window silently while they are retained, so the counts have to
// be recomputed.
func (s *activeSeriesStripe) updateMetrics(keepUntil time.Time, j judgement) {
	var tracked, active, owned, activeNativeHistogram int
	keepUntilNanos := keepUntil.UnixNano()

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, entries := range s.refs {
		tracked += len(entries)
		for i := range entries {
			if j.all || (j.pending && entries[i].createdRead >= j.pendingFrom) {
				entries[i].owned = j.owned(&entries[i])
			}

			if entries[i].owned {
				owned++
			}

			if !entries[i].seeded && entries[i].nanos.Load() >= keepUntilNanos {
				active++
				if entries[i].isNativeHistogram {
					activeNativeHistogram++
				}
			}
		}
	}

	s.storeCounts(int64(tracked), int64(active), int64(activeNativeHistogram), int64(owned))
}

// purge removes entries last updated before deleteBefore and recounts this
// stripe, counting as active the entries updated since activeCutoff.
func (s *activeSeriesStripe) purge(deleteBefore, activeCutoff time.Time) {
	var tracked, active, owned, activeNativeHistogram int
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
			tracked++
			if entries[0].owned {
				owned++
			}
			if !entries[0].seeded && ts >= activeCutoffNanos {
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
				tracked++
				if entries[i].owned {
					owned++
				}
				if !entries[i].seeded && ts >= activeCutoffNanos {
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
	s.storeCounts(int64(tracked), int64(active), int64(activeNativeHistogram), int64(owned))
}

// isOwned reports whether the series with the given ring token is owned by this
// instance, meaning this instance is one of the replicas the ring selects for it.
//
// The answer is a binary search over the ring's token list followed by an array
// index into a bitmap precomputed once per ring change.
//
// When ownership is unknown, because the ring has not been read yet or the bitmap
// does not match the token list, this returns true. Ownership is used to decide
// what counts against a limit, so the safe direction is to assume the series is
// ours: over-counting can only throttle the tenant as if owned series were not
// tracked, while under-counting would let it exceed its limit.
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
