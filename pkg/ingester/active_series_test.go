package ingester

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func copyFn(l labels.Labels) labels.Labels { return l }

func fromLabelToLabels(ls []labels.Label) labels.Labels {
	return *(*labels.Labels)(unsafe.Pointer(&ls))
}

// noRingToken is the ring token passed by tests which are not exercising
// ownership. With no ring loaded, ownership is unknown and every series counts as
// owned, so these tests observe exactly the behaviour that predates owned-series
// tracking.
const noRingToken = uint32(0)

// --- Active series behaviour. These tests predate owned-series tracking and are
// --- kept unchanged so that they continue to pin the active count's behaviour.

func TestActiveSeries_UpdateSeries(t *testing.T) {
	ls1 := []labels.Label{{Name: "a", Value: "1"}}
	ls2 := []labels.Label{{Name: "a", Value: "2"}}

	c := NewActiveSeries()
	assert.Equal(t, 0, c.Active())
	assert.Equal(t, 0, c.ActiveNativeHistogram())
	labels1Hash := fromLabelToLabels(ls1).Hash()
	labels2Hash := fromLabelToLabels(ls2).Hash()
	c.UpdateSeries(fromLabelToLabels(ls1), labels1Hash, noRingToken, time.Now(), true, copyFn)
	assert.Equal(t, 1, c.Active())
	assert.Equal(t, 1, c.ActiveNativeHistogram())

	c.UpdateSeries(fromLabelToLabels(ls1), labels1Hash, noRingToken, time.Now(), true, copyFn)
	assert.Equal(t, 1, c.Active())
	assert.Equal(t, 1, c.ActiveNativeHistogram())

	c.UpdateSeries(fromLabelToLabels(ls2), labels2Hash, noRingToken, time.Now(), true, copyFn)
	assert.Equal(t, 2, c.Active())
	assert.Equal(t, 2, c.ActiveNativeHistogram())
}

func TestActiveSeries_Purge(t *testing.T) {
	series := [][]labels.Label{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
		// The two following series have the same Fingerprint
		{{Name: "_", Value: "ypfajYg2lsv"}, {Name: "__name__", Value: "logs"}},
		{{Name: "_", Value: "KiqbryhzUpn"}, {Name: "__name__", Value: "logs"}},
	}

	// Run the same test for increasing TTL values
	for ttl := range series {
		c := NewActiveSeries()

		for i := range series {
			c.UpdateSeries(fromLabelToLabels(series[i]), fromLabelToLabels(series[i]).Hash(), noRingToken, time.Unix(int64(i), 0), true, copyFn)
		}

		c.Purge(time.Unix(int64(ttl+1), 0), time.Unix(int64(ttl+1), 0))
		// call purge twice, just to hit "quick" path. It doesn't really do anything.
		c.Purge(time.Unix(int64(ttl+1), 0), time.Unix(int64(ttl+1), 0))

		exp := len(series) - (ttl + 1)
		assert.Equal(t, exp, c.Active())
		assert.Equal(t, exp, c.ActiveNativeHistogram())
	}
}

func TestActiveSeries_PurgeOpt(t *testing.T) {
	metric := labels.NewBuilder(labels.FromStrings("__name__", "logs"))
	ls1 := metric.Set("_", "ypfajYg2lsv").Labels()
	ls2 := metric.Set("_", "KiqbryhzUpn").Labels()
	c := NewActiveSeries()

	now := time.Now()
	c.UpdateSeries(ls1, ls1.Hash(), noRingToken, now.Add(-2*time.Minute), true, copyFn)
	c.UpdateSeries(ls2, ls2.Hash(), noRingToken, now, true, copyFn)
	c.Purge(now, now)

	assert.Equal(t, 1, c.Active())
	assert.Equal(t, 1, c.ActiveNativeHistogram())

	c.UpdateSeries(ls1, ls1.Hash(), noRingToken, now.Add(-1*time.Minute), true, copyFn)
	c.UpdateSeries(ls2, ls2.Hash(), noRingToken, now, true, copyFn)
	c.Purge(now, now)

	assert.Equal(t, 1, c.Active())
	assert.Equal(t, 1, c.ActiveNativeHistogram())

	// This will *not* update the series, since there is already newer timestamp.
	c.UpdateSeries(ls2, ls2.Hash(), noRingToken, now.Add(-1*time.Minute), true, copyFn)
	c.Purge(now, now)

	assert.Equal(t, 1, c.Active())
	assert.Equal(t, 1, c.ActiveNativeHistogram())
}

// --- Ownership helpers.

// ownedPositionsFor builds an ownership bitmap over ringTokens from the subset of
// ring tokens this instance is a replica for. It lets these tests express
// ownership in terms of tokens, which reads more naturally, while the production
// code consumes the bitmap that ring.OwnedTokenPositions produces.
func ownedPositionsFor(ringTokens []uint32, ownedTokens ...uint32) []bool {
	ownedSet := make(map[uint32]struct{}, len(ownedTokens))
	for _, token := range ownedTokens {
		ownedSet[token] = struct{}{}
	}

	positions := make([]bool, len(ringTokens))
	for position, token := range ringTokens {
		_, positions[position] = ownedSet[token]
	}

	return positions
}

// testRingFingerprint derives a fingerprint from ring state the way the lifecycler
// does, so that tests passing identical state are seen as unchanged and tests
// passing different state are seen as a change.
func testRingFingerprint(ringTokens []uint32, ownedPositions []bool) uint64 {
	h := fnv.New64a()

	var buf [4]byte
	for _, token := range ringTokens {
		binary.LittleEndian.PutUint32(buf[:], token)
		_, _ = h.Write(buf[:])
	}
	for _, owned := range ownedPositions {
		if owned {
			_, _ = h.Write([]byte{1})
		} else {
			_, _ = h.Write([]byte{0})
		}
	}

	return h.Sum64()
}

// setRingState installs ring ownership state on c, reporting whether ownership
// changed.
func setRingState(c *ActiveSeries, ringTokens []uint32, ownedTokens ...uint32) bool {
	positions := ownedPositionsFor(ringTokens, ownedTokens...)
	return c.updateTokens(ringTokens, positions, testRingFingerprint(ringTokens, positions))
}

// updateMetricsWithRing calls UpdateMetrics with ownership expressed as the subset
// of ring tokens this instance is a replica for.
func updateMetricsWithRing(c *ActiveSeries, keepUntil time.Time, ringTokens []uint32, ownedTokens ...uint32) {
	positions := ownedPositionsFor(ringTokens, ownedTokens...)
	c.UpdateMetrics(keepUntil, ringTokens, positions, testRingFingerprint(ringTokens, positions))
}

// sumStripes totals the per-stripe counters, which are the source the cached
// totals are derived from.
func sumStripes(c *ActiveSeries) (active, owned, activeNativeHistogram int) {
	for s := range numActiveSeriesStripes {
		stripe := &c.stripes[s]
		stripe.mu.RLock()
		active += int(stripe.active.Load())
		owned += int(stripe.owned.Load())
		activeNativeHistogram += int(stripe.activeNativeHistogram.Load())
		stripe.mu.RUnlock()
	}
	return active, owned, activeNativeHistogram
}

func TestIsOwned(t *testing.T) {
	// Ring with 4 tokens across 2 ingesters. With a replication factor of 1 each
	// token range has a single owner, which is what makes the expectations below
	// a simple alternation.
	ringTokens := []uint32{100, 200, 300, 400}
	ingester0 := ownedPositionsFor(ringTokens, 100, 300)
	ingester1 := ownedPositionsFor(ringTokens, 200, 400)

	tests := []struct {
		name           string
		key            uint32
		ownedPositions []bool
		expected       bool
	}{
		// Hash 50 → SearchToken finds 100 → ingester-0 owns it
		{"hash 50 owned by ingester-0", 50, ingester0, true},
		{"hash 50 not owned by ingester-1", 50, ingester1, false},
		// Hash 150 → SearchToken finds 200 → ingester-1 owns it
		{"hash 150 owned by ingester-1", 150, ingester1, true},
		{"hash 150 not owned by ingester-0", 150, ingester0, false},
		// Hash 250 → SearchToken finds 300 → ingester-0 owns it
		{"hash 250 owned by ingester-0", 250, ingester0, true},
		{"hash 250 not owned by ingester-1", 250, ingester1, false},
		// Hash 350 → SearchToken finds 400 → ingester-1 owns it
		{"hash 350 owned by ingester-1", 350, ingester1, true},
		{"hash 350 not owned by ingester-0", 350, ingester0, false},
		// Hash 450 → wraps around → SearchToken finds 100 → ingester-0 owns it
		{"hash 450 wraps to ingester-0", 450, ingester0, true},
		{"hash 450 wraps, not ingester-1", 450, ingester1, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, isOwned(tc.key, ringTokens, tc.ownedPositions))
		})
	}
}

// TestIsOwned_UnknownOwnership covers the inputs which previously indexed a token
// slice without checking its length, which panicked on an empty ring.
func TestIsOwned_UnknownOwnership(t *testing.T) {
	assert.True(t, isOwned(50, nil, nil), "empty ring must not panic and must not under-count")
	assert.True(t, isOwned(50, []uint32{}, []bool{}), "empty ring must not panic and must not under-count")
	assert.True(t, isOwned(50, []uint32{100, 200}, nil), "missing bitmap must not under-count")
	assert.True(t, isOwned(50, []uint32{100, 200}, []bool{true}), "mismatched bitmap must not under-count")
}

// TestActiveSeries_ActiveIsUnaffectedByOwnership is the regression test for the
// property that owned-series tracking must not disturb the pre-existing
// cortex_ingester_active_series gauge. The same series are pushed into two
// trackers, one with a ring loaded where half the tokens belong elsewhere and one
// with no ring at all, and the active counts must agree exactly.
func TestActiveSeries_ActiveIsUnaffectedByOwnership(t *testing.T) {
	now := time.Now()
	ringTokens := []uint32{100, 200}

	withRing := NewActiveSeries()
	setRingState(withRing, ringTokens, 100) // owns token 100 only

	withoutRing := NewActiveSeries()

	// Keys alternate between the owned and the unowned token range.
	for i := range 20 {
		lbls := labels.FromStrings("__name__", "metric", "i", strconv.Itoa(i))
		key := uint32(50)
		if i%2 == 1 {
			key = 150
		}

		withRing.UpdateSeries(lbls, lbls.Hash(), key, now, i%3 == 0, copyFn)
		withoutRing.UpdateSeries(lbls, lbls.Hash(), key, now, i%3 == 0, copyFn)
	}

	assert.Equal(t, withoutRing.Active(), withRing.Active(),
		"ownership tracking must not change the active series count")
	assert.Equal(t, withoutRing.ActiveNativeHistogram(), withRing.ActiveNativeHistogram(),
		"ownership tracking must not change the active native histogram count")
	assert.Equal(t, 20, withRing.Active())

	// Ownership is the only thing that differs between the two.
	assert.Equal(t, 10, withRing.Owned(), "half the keys fall in a token range owned elsewhere")
	assert.Equal(t, 20, withoutRing.Owned(), "with no ring, ownership is unknown and everything counts")
}

func TestActiveSeries_OwnedCount_NoRingLoaded(t *testing.T) {
	// Before the ring is read, ownership is unknown, so Owned() equals Active().
	c := NewActiveSeries()
	now := time.Now()

	lbls1 := labels.FromStrings("__name__", "metric_1", "job", "test")
	lbls2 := labels.FromStrings("__name__", "metric_2", "job", "test")

	c.UpdateSeries(lbls1, lbls1.Hash(), noRingToken, now, false, copyFn)
	c.UpdateSeries(lbls2, lbls2.Hash(), noRingToken, now, false, copyFn)

	assert.Equal(t, 2, c.Active())
	assert.Equal(t, 2, c.Owned())
}

func TestActiveSeries_OwnedCount_WithRingLoaded(t *testing.T) {
	// With the ring loaded, an unowned series is still tracked as active but does
	// not count towards owned.
	c := NewActiveSeries()
	now := time.Now()

	ringTokens := []uint32{100, 200}
	setRingState(c, ringTokens, 100) // owns token 100

	// key=50 → SearchToken finds 100 → owned
	lblsOwned := labels.FromStrings("__name__", "metric_owned", "job", "test")
	c.UpdateSeries(lblsOwned, lblsOwned.Hash(), 50, now, false, copyFn)

	// key=150 → SearchToken finds 200 → not owned
	lblsUnowned := labels.FromStrings("__name__", "metric_not_owned", "job", "test")
	c.UpdateSeries(lblsUnowned, lblsUnowned.Hash(), 150, now, false, copyFn)

	assert.Equal(t, 2, c.Active(), "both series are active regardless of ownership")
	assert.Equal(t, 1, c.Owned(), "only the owned series counts towards owned")
}

// TestActiveSeries_OwnedExceedsActiveForIdleSeries covers the behaviour the whole
// feature rests on. Entries are retained until Purge is called with the head's
// minimum time, so a series which has gone idle still counts towards owned while
// it is still held in the head. owned therefore tracks what is in memory, which is
// what the series limit is protecting, rather than what is in the idle window.
func TestActiveSeries_OwnedExceedsActiveForIdleSeries(t *testing.T) {
	c := NewActiveSeries()
	now := time.Now()
	idleCutoff := now.Add(-10 * time.Minute)

	ringTokens := []uint32{100}
	setRingState(c, ringTokens, 100) // owns everything

	// One recent series and three which have gone idle.
	recent := labels.FromStrings("__name__", "recent")
	c.UpdateSeries(recent, recent.Hash(), 50, now, false, copyFn)
	for i := range 3 {
		lbls := labels.FromStrings("__name__", "idle", "i", strconv.Itoa(i))
		c.UpdateSeries(lbls, lbls.Hash(), 50, now.Add(-time.Hour), false, copyFn)
	}

	updateMetricsWithRing(c, idleCutoff, ringTokens, 100)

	assert.Equal(t, 1, c.Active(), "only the recent series is inside the idle window")
	assert.Equal(t, 4, c.Owned(), "all four are still held, so all four count towards owned")
	assert.Greater(t, c.Owned(), c.Active(), "owned exceeds active for an idle-heavy tenant")
}

// TestActiveSeries_UpdateMetricsRetainsExpiredEntries pins down that the periodic
// cycle removes nothing, which is what allows owned to outlive the idle window.
func TestActiveSeries_UpdateMetricsRetainsExpiredEntries(t *testing.T) {
	c := NewActiveSeries()
	now := time.Now()
	idleCutoff := now.Add(-30 * time.Minute)

	ringTokens := []uint32{100}
	setRingState(c, ringTokens, 100)

	old := labels.FromStrings("__name__", "old_metric")
	c.UpdateSeries(old, old.Hash(), 50, now.Add(-time.Hour), false, copyFn)
	recent := labels.FromStrings("__name__", "recent_metric")
	c.UpdateSeries(recent, recent.Hash(), 50, now, false, copyFn)

	updateMetricsWithRing(c, idleCutoff, ringTokens, 100)

	assert.Equal(t, 1, c.Active())
	assert.Equal(t, 2, c.Owned(), "the expired entry is retained and still owned")

	// Repeated cycles must be stable rather than progressively dropping entries.
	updateMetricsWithRing(c, idleCutoff, ringTokens, 100)
	assert.Equal(t, 1, c.Active())
	assert.Equal(t, 2, c.Owned())

	// Only a purge releases it.
	c.Purge(idleCutoff, idleCutoff)
	assert.Equal(t, 1, c.Active())
	assert.Equal(t, 1, c.Owned())
}

// TestActiveSeries_RingChangeMovesOwnedNotActive checks that losing ownership of a
// series changes only the owned count. The series is still held in this ingester's
// head, so it must remain active and must not be deleted.
func TestActiveSeries_RingChangeMovesOwnedNotActive(t *testing.T) {
	c := NewActiveSeries()
	now := time.Now()
	idleCutoff := now.Add(-10 * time.Minute)

	setRingState(c, []uint32{100, 200}, 100)

	lbls := labels.FromStrings("__name__", "metric_1", "job", "test")
	c.UpdateSeries(lbls, lbls.Hash(), 50, now, false, copyFn)

	assert.Equal(t, 1, c.Active())
	assert.Equal(t, 1, c.Owned())

	// The ring changes and token 100's range now belongs elsewhere.
	updateMetricsWithRing(c, idleCutoff, []uint32{100, 200, 300}, 200)

	assert.Equal(t, 1, c.Active(), "the series is still held here, so it is still active")
	assert.Equal(t, 0, c.Owned(), "but it is no longer owned")

	// And ownership can come back without the series having to be re-pushed.
	updateMetricsWithRing(c, idleCutoff, []uint32{100, 200, 300}, 100)
	assert.Equal(t, 1, c.Active())
	assert.Equal(t, 1, c.Owned())
}

// TestActiveSeries_CachedTotalsMatchStripes guards the cached totals that
// PreCreation reads. They are incremented as series are created and recomputed by
// the periodic cycle, so a mismatch would mean limits are enforced against a
// number that has drifted from reality.
func TestActiveSeries_CachedTotalsMatchStripes(t *testing.T) {
	c := NewActiveSeries()
	now := time.Now()
	ringTokens := []uint32{100, 200}
	setRingState(c, ringTokens, 100)

	for i := range 50 {
		lbls := labels.FromStrings("__name__", "metric", "i", strconv.Itoa(i))
		key := uint32(50)
		if i%2 == 1 {
			key = 150
		}
		c.UpdateSeries(lbls, lbls.Hash(), key, now, i%5 == 0, copyFn)
	}

	assertTotalsMatchStripes := func(stage string) {
		t.Helper()
		active, owned, activeNativeHistogram := sumStripes(c)
		assert.Equal(t, active, c.Active(), "active total drifted from stripes after %s", stage)
		assert.Equal(t, owned, c.Owned(), "owned total drifted from stripes after %s", stage)
		assert.Equal(t, activeNativeHistogram, c.ActiveNativeHistogram(), "native histogram total drifted from stripes after %s", stage)
	}

	assertTotalsMatchStripes("creation")

	updateMetricsWithRing(c, now.Add(-time.Minute), ringTokens, 100)
	assertTotalsMatchStripes("periodic cycle")

	c.Purge(now.Add(-time.Minute), now.Add(-time.Minute))
	assertTotalsMatchStripes("purge")

	c.clear()
	assertTotalsMatchStripes("clear")
	assert.Equal(t, 0, c.Active())
	assert.Equal(t, 0, c.Owned())
}

func TestActiveSeries_NativeHistogram_Owned(t *testing.T) {
	c := NewActiveSeries()
	now := time.Now()

	ringTokens := []uint32{100}
	setRingState(c, ringTokens, 100)

	lbls := labels.FromStrings("__name__", "histogram_metric", "job", "test")
	c.UpdateSeries(lbls, lbls.Hash(), 50, now, true, copyFn)

	assert.Equal(t, 1, c.Active())
	assert.Equal(t, 1, c.Owned())
	assert.Equal(t, 1, c.ActiveNativeHistogram())
}

func TestActiveSeries_ExistingSeriesKeepsOwnership(t *testing.T) {
	// A second sample for a series already tracked must not duplicate it, and must
	// not disturb its ownership.
	c := NewActiveSeries()
	now := time.Now()
	later := now.Add(1 * time.Minute)

	setRingState(c, []uint32{100, 200}, 100)

	lbls := labels.FromStrings("__name__", "metric_1", "job", "test")
	c.UpdateSeries(lbls, lbls.Hash(), 50, now, false, copyFn)
	require.Equal(t, 1, c.Active())
	require.Equal(t, 1, c.Owned())

	c.UpdateSeries(lbls, lbls.Hash(), 50, later, false, copyFn)
	assert.Equal(t, 1, c.Active())
	assert.Equal(t, 1, c.Owned())
}

func TestUpdateTokens_DetectsChange(t *testing.T) {
	c := NewActiveSeries()

	// First call should detect change (from empty to something)
	assert.True(t, setRingState(c, []uint32{100, 200}, 100))

	// Same state again — no change
	assert.False(t, setRingState(c, []uint32{100, 200}, 100))

	// Different ring tokens — change detected
	assert.True(t, setRingState(c, []uint32{100, 200, 300}, 100))

	// Same tokens but different ownership — must still be detected, because a peer
	// changing state alters the replica set without moving any token.
	assert.True(t, setRingState(c, []uint32{100, 200, 300}, 100, 200))
}

func TestActiveSeries_UpdateTokens_ImmutableSnapshots(t *testing.T) {
	// Verify that updateTokens publishes a new ringState each time rather than
	// mutating the previous one, which is what makes the atomic.Pointer safe.
	c := NewActiveSeries()

	setRingState(c, []uint32{100, 200}, 100)
	state1 := c.ring.Load()

	setRingState(c, []uint32{100, 200, 300}, 100, 300)
	state2 := c.ring.Load()

	assert.NotEqual(t, state1, state2)
	assert.Len(t, state1.tokens, 2)
	assert.Len(t, state2.tokens, 3)
	assert.Len(t, state1.ownedPositions, 2)
	assert.Len(t, state2.ownedPositions, 3)
}

// --- Benchmarks. These predate owned-series tracking and are kept so that the
// --- push and purge paths stay comparable against earlier numbers.

var activeSeriesTestGoroutines = []int{50, 100, 500}

func BenchmarkActiveSeriesTest_single_series(b *testing.B) {
	for _, num := range activeSeriesTestGoroutines {
		b.Run(fmt.Sprintf("%d", num), func(b *testing.B) {
			benchmarkActiveSeriesConcurrencySingleSeries(b, num)
		})
	}
}

func benchmarkActiveSeriesConcurrencySingleSeries(b *testing.B, goroutines int) {
	series := labels.FromStrings("a", "a")

	c := NewActiveSeries()

	wg := &sync.WaitGroup{}
	start := make(chan struct{})
	max := int(math.Ceil(float64(b.N) / float64(goroutines)))
	labelhash := series.Hash()
	for range goroutines {
		wg.Go(func() {
			<-start

			now := time.Now()

			for ix := range max {
				now = now.Add(time.Duration(ix) * time.Millisecond)
				c.UpdateSeries(series, labelhash, noRingToken, now, false, copyFn)
			}
		})
	}

	b.ResetTimer()
	close(start)
	wg.Wait()
}

func BenchmarkActiveSeries_UpdateSeries(b *testing.B) {
	c := NewActiveSeries()

	// Prepare series
	nameBuf := bytes.Buffer{}
	for range 50 {
		nameBuf.WriteString("abcdefghijklmnopqrstuvzyx")
	}
	name := nameBuf.String()

	// NOTE: this deliberately does not use b.Loop(). A benchmark may only run one
	// b.Loop() loop, and sizing the series slice requires knowing the iteration
	// count before the loop starts, so the b.N form is the correct one here.
	series := make([]labels.Labels, b.N)
	labelhash := make([]uint64, b.N)
	for s := 0; s < b.N; s++ {
		series[s] = labels.FromStrings(name, name+strconv.Itoa(s))
		labelhash[s] = series[s].Hash()
	}

	now := time.Now().UnixNano()
	b.ResetTimer()

	for ix := 0; ix < b.N; ix++ {
		c.UpdateSeries(series[ix], labelhash[ix], noRingToken, time.Unix(0, now+int64(ix)), false, copyFn)
	}
}

func BenchmarkActiveSeries_Purge_once(b *testing.B) {
	benchmarkPurge(b, false)
}

func BenchmarkActiveSeries_Purge_twice(b *testing.B) {
	benchmarkPurge(b, true)
}

func benchmarkPurge(b *testing.B, twice bool) {
	const numSeries = 10000
	const numExpiresSeries = numSeries / 25

	now := time.Now()
	c := NewActiveSeries()

	series := [numSeries]labels.Labels{}
	labelhash := [numSeries]uint64{}
	for s := range numSeries {
		series[s] = labels.FromStrings("a", strconv.Itoa(s))
		labelhash[s] = series[s].Hash()
	}

	for b.Loop() {
		b.StopTimer()

		// Prepare series
		for ix, s := range series {
			if ix < numExpiresSeries {
				c.UpdateSeries(s, labelhash[ix], noRingToken, now.Add(-time.Minute), false, copyFn)
			} else {
				c.UpdateSeries(s, labelhash[ix], noRingToken, now, false, copyFn)
			}
		}

		assert.Equal(b, numSeries, c.Active())
		b.StartTimer()

		// Purge everything
		c.Purge(now, now)
		assert.Equal(b, numSeries-numExpiresSeries, c.Active())

		if twice {
			c.Purge(now, now)
			assert.Equal(b, numSeries-numExpiresSeries, c.Active())
		}
	}
}

// --- Benchmarks for the owned-series paths.

// BenchmarkActiveSeries_UpdateSeries_Owned measures the push path with a ring
// loaded, which is the cost ownership tracking adds per new series: one binary
// search over the ring's tokens plus one array index.
func BenchmarkActiveSeries_UpdateSeries_Owned(b *testing.B) {
	const numRingTokens = 100 * 512

	ringTokens := make([]uint32, numRingTokens)
	for i := range ringTokens {
		ringTokens[i] = uint32(i) * 128
	}
	ownedPositions := make([]bool, numRingTokens)
	for i := range ownedPositions {
		ownedPositions[i] = i%3 == 0
	}

	for _, withRing := range []bool{false, true} {
		name := "ring_not_loaded"
		if withRing {
			name = "ring_loaded"
		}

		b.Run(name, func(b *testing.B) {
			c := NewActiveSeries()
			if withRing {
				c.updateTokens(ringTokens, ownedPositions, 1)
			}

			series := make([]labels.Labels, b.N)
			labelhash := make([]uint64, b.N)
			for s := 0; s < b.N; s++ {
				series[s] = labels.FromStrings("__name__", "metric", "i", strconv.Itoa(s))
				labelhash[s] = series[s].Hash()
			}

			now := time.Now()
			b.ReportAllocs()
			b.ResetTimer()

			for ix := 0; ix < b.N; ix++ {
				c.UpdateSeries(series[ix], labelhash[ix], uint32(ix)*7919, now, false, copyFn)
			}
		})
	}
}

// BenchmarkActiveSeries_UpdateMetrics measures the periodic recount, separating the
// common case where the ring has not changed from the case where every entry's
// ownership has to be re-evaluated.
func BenchmarkActiveSeries_UpdateMetrics(b *testing.B) {
	const (
		numSeries     = 100000
		numRingTokens = 1000
	)

	ringTokens := make([]uint32, numRingTokens)
	for i := range ringTokens {
		ringTokens[i] = uint32(i) * 4096
	}
	ownedPositions := make([]bool, numRingTokens)
	for i := range ownedPositions {
		ownedPositions[i] = i%3 == 0
	}

	now := time.Now()
	c := NewActiveSeries()
	c.updateTokens(ringTokens, ownedPositions, 1)
	for i := range numSeries {
		lbls := labels.FromStrings("__name__", "metric", "i", strconv.Itoa(i))
		c.UpdateSeries(lbls, lbls.Hash(), uint32(i)*7919, now, false, copyFn)
	}

	b.Run("ring_unchanged", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			c.UpdateMetrics(now.Add(-time.Minute), ringTokens, ownedPositions, 1)
		}
	})

	b.Run("ring_changed", func(b *testing.B) {
		b.ReportAllocs()
		fingerprint := uint64(1)
		for b.Loop() {
			fingerprint++
			c.UpdateMetrics(now.Add(-time.Minute), ringTokens, ownedPositions, fingerprint)
		}
	})
}

// TestActiveSeriesPurgeCutoff covers the clock-domain hazard in head-anchored
// retention. Entry timestamps are wall-clock arrival times while the head's minimum
// time is a sample timestamp, so a tenant whose sample timestamps run ahead of real
// time would otherwise produce a cutoff in the recent past and drop entries for
// series which are still resident, under-counting owned series and letting the
// tenant exceed its limit.
func TestActiveSeriesPurgeCutoff(t *testing.T) {
	const blockRange = 2 * time.Hour
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)

	tests := map[string]struct {
		headMinTime time.Time
		expected    time.Time
		why         string
	}{
		"real-time ingestion uses the head's minimum time": {
			headMinTime: now.Add(-blockRange),
			expected:    now.Add(-blockRange),
			why:         "sample time and arrival time agree, so no clamping applies",
		},
		"backdated samples retain entries for longer": {
			headMinTime: now.Add(-3 * time.Hour),
			expected:    now.Add(-3 * time.Hour),
			why:         "an earlier cutoff only over-retains, which is the safe direction",
		},
		"future-dated samples are clamped": {
			headMinTime: now.Add(10 * time.Minute),
			expected:    now.Add(-blockRange),
			why:         "without clamping this would drop every entry, since no arrival time is in the future",
		},
		"a head minimum time inside the block range is clamped": {
			headMinTime: now.Add(-30 * time.Minute),
			expected:    now.Add(-blockRange),
			why:         "entries for series admitted up to a block range ago must survive",
		},
		"a head minimum time exactly one block range ago is kept": {
			headMinTime: now.Add(-blockRange),
			expected:    now.Add(-blockRange),
			why:         "the boundary itself is not clamped",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := activeSeriesPurgeCutoff(tc.headMinTime.UnixMilli(), now, blockRange)
			assert.Equal(t, tc.expected.UTC(), got.UTC(), tc.why)
			assert.False(t, got.After(now.Add(-blockRange)),
				"the cutoff must never be more recent than one block range ago")
		})
	}
}

// TestActiveSeriesPurgeCutoff_NeverDropsResidentSeries states the property the clamp
// exists for: whatever the head reports, an entry whose series arrived within the
// last block range is never released.
func TestActiveSeriesPurgeCutoff_NeverDropsResidentSeries(t *testing.T) {
	const blockRange = 2 * time.Hour
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)

	// A series which received a sample just now, and one which received a sample
	// almost a whole block range ago. Both could still be in the head.
	justArrived := now
	nearlyStale := now.Add(-blockRange).Add(time.Minute)

	for _, skew := range []time.Duration{
		-24 * time.Hour, -3 * time.Hour, -blockRange, -time.Minute, 0, time.Minute, 10 * time.Minute, time.Hour,
	} {
		cutoff := activeSeriesPurgeCutoff(now.Add(skew).UnixMilli(), now, blockRange)

		assert.True(t, justArrived.After(cutoff),
			"a series that just arrived must survive a head minimum time skewed by %s", skew)
		assert.True(t, nearlyStale.After(cutoff),
			"a series that arrived within the block range must survive a head minimum time skewed by %s", skew)
	}
}

// TestActiveSeries_ReactivatedSeriesCountsImmediately covers the one way retaining
// entries could have changed the active count's behaviour.
//
// Before entries were retained, a series going idle had its entry removed, so a later
// sample created a fresh entry and active reflected it at once. Now the entry survives
// but stops being counted, and a later sample updates it in place rather than creating
// anything. Without explicit handling, active would not notice the series had returned
// until the next periodic recount, under-reporting for up to one update period.
func TestActiveSeries_ReactivatedSeriesCountsImmediately(t *testing.T) {
	now := time.Now()
	idleCutoff := now.Add(-10 * time.Minute)
	ringTokens := []uint32{100}

	for _, nativeHistogram := range []bool{false, true} {
		name := "sample"
		if nativeHistogram {
			name = "native histogram"
		}

		t.Run(name, func(t *testing.T) {
			c := NewActiveSeries()
			setRingState(c, ringTokens, 100)

			// A series whose last sample is older than the idle window.
			lbls := labels.FromStrings("__name__", "idle_metric")
			c.UpdateSeries(lbls, lbls.Hash(), 50, now.Add(-time.Hour), nativeHistogram, copyFn)

			updateMetricsWithRing(c, idleCutoff, ringTokens, 100)
			require.Equal(t, 0, c.Active(), "the series is outside the idle window")
			require.Equal(t, 0, c.ActiveNativeHistogram())
			require.Equal(t, 1, c.Owned(), "but it is still held, so it is still owned")

			// It receives a sample again. No entry is created, because the entry was
			// retained, so this has to be noticed explicitly.
			c.UpdateSeries(lbls, lbls.Hash(), 50, now, nativeHistogram, copyFn)

			assert.Equal(t, 1, c.Active(), "active must reflect the returning series without waiting for a recount")
			assert.Equal(t, 1, c.Owned(), "owned is unchanged: an idle series never stopped being owned")
			if nativeHistogram {
				assert.Equal(t, 1, c.ActiveNativeHistogram())
			} else {
				assert.Equal(t, 0, c.ActiveNativeHistogram())
			}

			// Further samples inside the window must not count it again.
			c.UpdateSeries(lbls, lbls.Hash(), 50, now.Add(time.Second), nativeHistogram, copyFn)
			assert.Equal(t, 1, c.Active(), "a series already inside the window is not counted twice")

			// And the authoritative recount must agree with what the push path did.
			updateMetricsWithRing(c, idleCutoff, ringTokens, 100)
			assert.Equal(t, 1, c.Active())
			assert.Equal(t, 1, c.Owned())

			active, owned, activeNH := sumStripes(c)
			assert.Equal(t, active, c.Active(), "cached active drifted from the stripes")
			assert.Equal(t, owned, c.Owned(), "cached owned drifted from the stripes")
			assert.Equal(t, activeNH, c.ActiveNativeHistogram(), "cached native histogram total drifted from the stripes")
		})
	}
}

// TestActiveSeries_ReactivationIsCountedOnceUnderConcurrency checks that concurrent
// samples for the same returning series only count it once, which relies on the
// compare-and-swap that moves the timestamp across the cutoff arbitrating.
func TestActiveSeries_ReactivationIsCountedOnceUnderConcurrency(t *testing.T) {
	now := time.Now()
	idleCutoff := now.Add(-10 * time.Minute)
	ringTokens := []uint32{100}

	c := NewActiveSeries()
	setRingState(c, ringTokens, 100)

	lbls := labels.FromStrings("__name__", "idle_metric")
	c.UpdateSeries(lbls, lbls.Hash(), 50, now.Add(-time.Hour), false, copyFn)
	updateMetricsWithRing(c, idleCutoff, ringTokens, 100)
	require.Equal(t, 0, c.Active())

	const goroutines = 64
	start := make(chan struct{})
	wg := &sync.WaitGroup{}
	for i := range goroutines {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			c.UpdateSeries(lbls, lbls.Hash(), 50, now.Add(time.Duration(i)*time.Millisecond), false, copyFn)
		}(i)
	}
	close(start)
	wg.Wait()

	assert.Equal(t, 1, c.Active(), "concurrent samples for one returning series must count it once")
	assert.Equal(t, 1, c.Owned())

	active, owned, _ := sumStripes(c)
	assert.Equal(t, active, c.Active())
	assert.Equal(t, owned, c.Owned())
}

// TestActiveSeries_CountsAreExactUnderConcurrentRecount asserts that a periodic
// recount running concurrently with series creation neither loses nor double counts.
// Every counter mutation happens while holding the relevant stripe's lock, and the
// totals are summed from the stripes rather than cached separately, so the two
// cannot disagree.
//
// Note this does not reproduce the transient drift of a design that caches one
// cross-stripe total: there, a creation landing between a stripe being scanned and
// the total being stored is lost, but the next complete recount repairs it, so the
// window is not observable from a test that asserts after the fact. This test pins
// the steady-state invariant instead.
func TestActiveSeries_CountsAreExactUnderConcurrentRecount(t *testing.T) {
	const (
		writers         = 16
		seriesPerWriter = 400
		totalSeries     = writers * seriesPerWriter
	)

	now := time.Now()
	idleCutoff := now.Add(-10 * time.Minute)
	ringTokens := []uint32{100}

	c := NewActiveSeries()
	setRingState(c, ringTokens, 100)

	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
		done  = make(chan struct{})
	)

	// Recount continuously while series are being created.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for {
			select {
			case <-done:
				return
			default:
				updateMetricsWithRing(c, idleCutoff, ringTokens, 100)
			}
		}
	}()

	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := range seriesPerWriter {
				lbls := labels.FromStrings("__name__", "metric", "w", strconv.Itoa(w), "i", strconv.Itoa(i))
				c.UpdateSeries(lbls, lbls.Hash(), 50, now, false, copyFn)
			}
		}(w)
	}

	close(start)
	// Let the writers finish, then stop the recounter.
	time.Sleep(50 * time.Millisecond)
	close(done)
	wg.Wait()

	// Every series was created once, inside the active window, and owned.
	assert.Equal(t, totalSeries, c.Active(), "active count drifted under a concurrent recount")
	assert.Equal(t, totalSeries, c.Owned(), "owned count drifted under a concurrent recount")

	active, owned, _ := sumStripes(c)
	assert.Equal(t, active, c.Active())
	assert.Equal(t, owned, c.Owned())

	// And a final quiescent recount must agree.
	updateMetricsWithRing(c, idleCutoff, ringTokens, 100)
	assert.Equal(t, totalSeries, c.Active())
	assert.Equal(t, totalSeries, c.Owned())
}

// TestActiveSeries_PurgeToHeadDoesNotInflateActive is the regression test for
// conflating the two cutoffs purge is given.
//
// With owned-series tracking on, purge is called at head compaction with a cutoff
// derived from the head's minimum time, which can be hours old. If that same cutoff
// were also used to decide what counts as active, every retained entry would be
// counted, including ones idle for hours, so the active count would jump to the
// owned count until the next periodic recount. Two readers would see it: the native
// histogram limit on the PreCreation path, and the per-tenant stats endpoint.
func TestActiveSeries_PurgeToHeadDoesNotInflateActive(t *testing.T) {
	now := time.Now()
	idleCutoff := now.Add(-10 * time.Minute)
	headCutoff := now.Add(-2 * time.Hour)
	ringTokens := []uint32{100}

	c := NewActiveSeries()
	setRingState(c, ringTokens, 100)

	// One recent series, three idle for an hour but still inside the head window.
	recent := labels.FromStrings("__name__", "recent")
	c.UpdateSeries(recent, recent.Hash(), 50, now, true, copyFn)
	for i := range 3 {
		lbls := labels.FromStrings("__name__", "idle", "i", strconv.Itoa(i))
		c.UpdateSeries(lbls, lbls.Hash(), 50, now.Add(-time.Hour), true, copyFn)
	}

	updateMetricsWithRing(c, idleCutoff, ringTokens, 100)
	require.Equal(t, 1, c.Active())
	require.Equal(t, 1, c.ActiveNativeHistogram())
	require.Equal(t, 4, c.Owned())

	// Purge at head compaction: nothing is old enough to delete.
	c.Purge(headCutoff, idleCutoff)

	assert.Equal(t, 1, c.Active(), "purging to the head cutoff must not count idle entries as active")
	assert.Equal(t, 1, c.ActiveNativeHistogram(), "the native histogram limit input must not be inflated")
	assert.Equal(t, 4, c.Owned(), "all four are still held, so all four are still owned")

	// Now move the head cutoff past the idle series: they are released, and both
	// counts follow.
	c.Purge(now.Add(-30*time.Minute), idleCutoff)
	assert.Equal(t, 1, c.Active())
	assert.Equal(t, 1, c.Owned())
}

// TestActiveSeries_TrackedRevealsAnUnseenHead covers the signal that keeps limit
// enforcement safe after a restart.
//
// Entries are only created when a sample arrives, so an ingester that has just
// replayed its WAL has a full head and an empty tracker. Owned() would read zero and
// the limit would admit a whole limit's worth of new series on top of everything
// already resident. Tracked() is what lets the caller notice.
func TestActiveSeries_TrackedRevealsAnUnseenHead(t *testing.T) {
	now := time.Now()
	idleCutoff := now.Add(-10 * time.Minute)
	ringTokens := []uint32{100, 200}

	c := NewActiveSeries()
	setRingState(c, ringTokens, 100)

	// Nothing pushed yet: this is the post-replay state.
	assert.Equal(t, 0, c.Tracked(), "a tracker that has seen no samples holds nothing")
	assert.Equal(t, 0, c.Owned(), "so Owned is zero even though a real head would be full")

	// Samples arrive. Tracked counts every series, owned only the ones we own.
	for i := range 10 {
		lbls := labels.FromStrings("__name__", "metric", "i", strconv.Itoa(i))
		key := uint32(50)
		if i%2 == 1 {
			key = 150 // falls in a token range owned elsewhere
		}
		c.UpdateSeries(lbls, lbls.Hash(), key, now, false, copyFn)
	}

	assert.Equal(t, 10, c.Tracked(), "tracked counts series regardless of ownership")
	assert.Equal(t, 10, c.Active())
	assert.Equal(t, 5, c.Owned())

	// Tracked survives the idle window, like owned does, because entries are retained.
	updateMetricsWithRing(c, now.Add(time.Minute), ringTokens, 100)
	assert.Equal(t, 10, c.Tracked(), "retained entries are still tracked once idle")
	assert.Equal(t, 0, c.Active(), "but none are active")
	assert.Equal(t, 5, c.Owned())

	// And drops only when entries are actually released.
	c.Purge(now.Add(time.Minute), idleCutoff)
	assert.Equal(t, 0, c.Tracked())
	assert.Equal(t, 0, c.Owned())
}
