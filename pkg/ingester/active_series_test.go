package ingester

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ring"
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
// ownership in terms of tokens, which reads more naturally, while the ingester
// itself consumes the bitmap that ring.Ring.OwnedTokenPositions produces.
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
	// The view of the ring is newer than any entry a test creates, which is
	// created at read 0, so every entry is judged.
	c.UpdateMetrics(keepUntil, ringTokens, positions, testRingFingerprint(ringTokens, positions), 1)
}

// sumStripes totals the per-stripe counters by visiting every stripe under its
// lock. The accessors read the cross-stripe totals instead, which are maintained
// by applying each mutation's delta, so comparing the two checks that no mutation
// updated a stripe without also updating the totals.
func sumStripes(c *ActiveSeries) (active, owned, activeNativeHistogram int) {
	active, owned, activeNativeHistogram, _ = sumStripesAll(c)
	return active, owned, activeNativeHistogram
}

func sumStripesAll(c *ActiveSeries) (active, owned, activeNativeHistogram, tracked int) {
	for s := range numActiveSeriesStripes {
		stripe := &c.stripes[s]
		stripe.mu.RLock()
		active += int(stripe.active.Load())
		owned += int(stripe.owned.Load())
		activeNativeHistogram += int(stripe.activeNativeHistogram.Load())
		tracked += int(stripe.tracked.Load())
		stripe.mu.RUnlock()
	}
	return active, owned, activeNativeHistogram, tracked
}

// requireTotalsMatchStripes checks the invariant the cross-stripe totals rest on:
// every counter equals the sum of the stripes it mirrors.
func requireTotalsMatchStripes(t *testing.T, c *ActiveSeries) {
	t.Helper()
	active, owned, activeNativeHistogram, tracked := sumStripesAll(c)
	assert.Equal(t, active, c.Active(), "active total disagrees with the stripes")
	assert.Equal(t, owned, c.Owned(), "owned total disagrees with the stripes")
	assert.Equal(t, activeNativeHistogram, c.ActiveNativeHistogram(), "native histogram total disagrees with the stripes")
	assert.Equal(t, tracked, c.Tracked(), "tracked total disagrees with the stripes")
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

	updateMetricsWithRing(withRing, now.Add(-time.Minute), ringTokens, 100) // owns token 100 only

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

	// key=50 → SearchToken finds 100 → owned
	lblsOwned := labels.FromStrings("__name__", "metric_owned", "job", "test")
	c.UpdateSeries(lblsOwned, lblsOwned.Hash(), 50, now, false, copyFn)

	// key=150 → SearchToken finds 200 → not owned
	lblsUnowned := labels.FromStrings("__name__", "metric_not_owned", "job", "test")
	c.UpdateSeries(lblsUnowned, lblsUnowned.Hash(), 150, now, false, copyFn)

	assert.Equal(t, 2, c.Owned(), "a series is owned when it arrives")

	updateMetricsWithRing(c, now.Add(-time.Minute), ringTokens, 100) // owns token 100
	assert.Equal(t, 2, c.Active(), "both series are active regardless of ownership")
	assert.Equal(t, 1, c.Owned(), "only the owned series counts towards owned")
}

// TestActiveSeries_OwnedExceedsActiveForIdleSeries covers the behaviour the whole
// feature rests on. Entries are retained until the head deletes their series, so
// a series which has gone idle still counts towards owned while it is still held
// in the head. owned therefore tracks what is in memory, which is
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

	// Only a purge, or the head deleting the series, releases it.
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

func TestActiveSeries_UpdateTokens_AdoptsNewSlicesForTheSameOwnership(t *testing.T) {
	c := NewActiveSeries()
	tokens, positions := []uint32{100, 200}, []bool{true, false}
	require.True(t, c.updateTokens(tokens, positions, 7))

	// A rebuilt ring hands over equal ownership in new slices, under the same
	// fingerprint. They are adopted, so the old ones can be freed, but nothing
	// is re-evaluated.
	rebuilt, rebuiltPositions := []uint32{100, 200}, []bool{true, false}
	require.False(t, c.updateTokens(rebuilt, rebuiltPositions, 7))
	require.Same(t, &rebuilt[0], &c.tokens[0])

	require.True(t, c.updateTokens([]uint32{100, 200, 300}, []bool{true, false, true}, 8))
	require.Len(t, c.tokens, 3)

	require.False(t, c.updateTokens(nil, nil, 9), "no tokens is no ownership")
	require.False(t, c.updateTokens([]uint32{1}, []bool{true}, 0), "zero is unknown ownership")
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
			c.UpdateMetrics(now.Add(-time.Minute), ringTokens, ownedPositions, 1, 1)
		}
	})

	b.Run("ring_changed", func(b *testing.B) {
		b.ReportAllocs()
		fingerprint := uint64(1)
		for b.Loop() {
			fingerprint++
			c.UpdateMetrics(now.Add(-time.Minute), ringTokens, ownedPositions, fingerprint, 1)
		}
	})
}

// BenchmarkActiveSeries_UpdateExistingSeries measures the update made for every
// sample of a series the tracker already holds, which is the push path's common
// case, from one goroutine and from many. Each goroutine starts at a different
// series, so that they do not walk the stripes in step.
func BenchmarkActiveSeries_UpdateExistingSeries(b *testing.B) {
	const numSeries = 100000

	c := NewActiveSeries()
	lbls := make([]labels.Labels, numSeries)
	hashes := make([]uint64, numSeries)
	now := time.Now()
	for i := range lbls {
		lbls[i] = labels.FromStrings("__name__", "metric", "i", strconv.Itoa(i))
		hashes[i] = lbls[i].Hash()
		c.UpdateSeries(lbls[i], hashes[i], noRingToken, now, false, copyFn)
	}

	b.Run("serial", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			j := i % numSeries
			c.UpdateSeries(lbls[j], hashes[j], noRingToken, now.Add(time.Duration(i)), false, copyFn)
		}
	})
	b.Run("parallel", func(b *testing.B) {
		var worker atomic.Int64
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for i := int(worker.Add(1)) * 104729; pb.Next(); i++ {
				j := (i * 7919) % numSeries
				c.UpdateSeries(lbls[j], hashes[j], noRingToken, now.Add(time.Duration(i)), false, copyFn)
			}
		})
	})
}

// BenchmarkActiveSeries_OwnedAndTracked measures the counts PreCreation reads for
// every new series when owned series limits are enforced. Each sums a counter
// over every stripe, so the cost does not depend on the number of series.
func BenchmarkActiveSeries_OwnedAndTracked(b *testing.B) {
	c := NewActiveSeries()
	now := time.Now()
	for i := range 100000 {
		lbls := labels.FromStrings("__name__", "metric", "i", strconv.Itoa(i))
		c.UpdateSeries(lbls, lbls.Hash(), uint32(i)*7919, now, false, copyFn)
	}

	b.ReportAllocs()
	for b.Loop() {
		_ = ownedSeriesLimitCount(100000, c.Owned(), c.Tracked())
	}
}

// BenchmarkActiveSeries_DeleteSeries measures releasing tracker entries when head
// compaction deletes their series, per deleted series.
func BenchmarkActiveSeries_DeleteSeries(b *testing.B) {
	const numSeries = 100000

	lbls := make([]labels.Labels, numSeries)
	for i := range lbls {
		lbls[i] = labels.FromStrings("__name__", "metric", "i", strconv.Itoa(i))
	}
	deleted := make(map[chunks.HeadSeriesRef]labels.Labels, numSeries)
	for i := range lbls {
		deleted[chunks.HeadSeriesRef(i+1)] = lbls[i]
	}
	ringTokens := []uint32{1 << 31, 1<<32 - 1}
	ownedPositions := []bool{true, false}
	now := time.Now()

	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		c := NewActiveSeries()
		c.updateTokens(ringTokens, ownedPositions, 1)
		for i := range lbls {
			c.UpdateSeriesWithRef(lbls[i], lbls[i].Hash(), uint64(i+1), knownSeriesToken(uint32(i)*7919), now, false, copyFn)
		}
		b.StartTimer()
		c.DeleteSeries(deleted)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*numSeries), "ns/series")
}

// BenchmarkActiveSeries_MemoryPerEntry reports the heap held per tracker entry,
// labels excluded since the tracker shares them with the head. With owned-series
// tracking an entry also carries its head series reference, and entries are kept
// until the head deletes the series rather than until the idle timeout, so the
// extra memory is roughly this per-entry cost times (series in the head - active
// series).
func BenchmarkActiveSeries_MemoryPerEntry(b *testing.B) {
	const n = 1_000_000

	lbls := make([]labels.Labels, n)
	for i := range lbls {
		lbls[i] = labels.FromStrings("__name__", "metric", "i", strconv.Itoa(i))
	}
	ringTokens := []uint32{1 << 31, 1<<32 - 1}
	ownedPositions := []bool{true, false}
	now := time.Now()

	for _, ownedTracking := range []bool{false, true} {
		name := "owned_tracking_disabled"
		if ownedTracking {
			name = "owned_tracking_enabled"
		}

		b.Run(name, func(b *testing.B) {
			for b.Loop() {
				var before, after runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&before)

				c := NewActiveSeries()
				if ownedTracking {
					c.updateTokens(ringTokens, ownedPositions, 1)
				}
				for i := range lbls {
					if ownedTracking {
						c.UpdateSeriesWithRef(lbls[i], lbls[i].Hash(), uint64(i+1), knownSeriesToken(uint32(i)*7919), now, false, copyFn)
					} else {
						c.UpdateSeries(lbls[i], lbls[i].Hash(), noRingToken, now, false, copyFn)
					}
				}

				runtime.GC()
				runtime.ReadMemStats(&after)
				b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/n, "B/entry")
				runtime.KeepAlive(c)
			}
		})
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
//
// This is the test that guards the cross-stripe totals. A revision of this file
// cached one total per counter and had the recount store an absolute sum computed
// from its scan, which lost any creation landing between the scan reading a stripe
// and the store. The totals are now moved by the same delta as the stripe they
// mirror, under that stripe's lock, which is what makes them exact; this test is
// what would catch a mutation that updated one and not the other.
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
	wg.Go(func() {
		<-start
		for {
			select {
			case <-done:
				return
			default:
				updateMetricsWithRing(c, idleCutoff, ringTokens, 100)
			}
		}
	})

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

	requireTotalsMatchStripes(t, c)

	// And a final quiescent recount must agree.
	updateMetricsWithRing(c, idleCutoff, ringTokens, 100)
	assert.Equal(t, totalSeries, c.Active())
	assert.Equal(t, totalSeries, c.Owned())
}

// TestActiveSeries_TotalsMatchStripesUnderEveryMutation drives every path that
// moves a counter, concurrently, and asserts the cross-stripe totals still equal
// the sum of the stripes.
//
// The previous test only creates series, so it exercises two of the four counters
// and one of the mutation paths. This one adds deletions, native histograms, the
// compaction backstop purge and a changing ring, because the totals are only as
// trustworthy as the least-used path that moves them: a single call site that
// updated a stripe without its delta would leave the totals permanently adrift,
// and the per-tenant limit reads the totals.
func TestActiveSeries_TotalsMatchStripesUnderEveryMutation(t *testing.T) {
	const (
		writers         = 8
		seriesPerWriter = 300
	)

	now := time.Now()
	idleCutoff := now.Add(-10 * time.Minute)
	ringTokens := []uint32{100, 200, 300, 400}

	c := NewActiveSeries()
	setRingState(c, ringTokens, 100, 300)

	var (
		writersWG sync.WaitGroup
		recountWG sync.WaitGroup
		start     = make(chan struct{})
		done      = make(chan struct{})
	)

	// Recount and purge continuously, alternating which positions are owned so
	// that the owned counter really moves. It is tracked separately from the
	// writers, because it only stops once done is closed and the writers have to
	// be finished before that.
	recountWG.Go(func() {
		<-start
		for i := 0; ; i++ {
			select {
			case <-done:
				return
			default:
			}
			if i%2 == 0 {
				updateMetricsWithRing(c, idleCutoff, ringTokens, 100, 300)
			} else {
				updateMetricsWithRing(c, idleCutoff, ringTokens, 200, 400)
			}
			// The backstop purge, with a cutoff far older than any entry here, so
			// it removes nothing but still recounts every stripe.
			c.Purge(now.Add(-4*time.Hour), c.activeCutoff())
		}
	})

	// Writers create series, half of them native histograms, then delete a third
	// of what they created through the head's deletion callback.
	for w := range writers {
		writersWG.Add(1)
		go func(w int) {
			defer writersWG.Done()
			<-start
			created := make([]labels.Labels, 0, seriesPerWriter)
			for i := range seriesPerWriter {
				lbls := labels.FromStrings("__name__", "metric", "w", strconv.Itoa(w), "i", strconv.Itoa(i))
				ref := uint64(w*seriesPerWriter + i + 1)
				// Vary the token so entries land on different ring positions, and
				// so ownership differs between the two bitmaps above.
				token := knownSeriesToken(uint32(50 + (i%4)*100))
				c.UpdateSeriesWithRef(lbls, lbls.Hash(), ref, token, now, i%2 == 0, copyFn)
				created = append(created, lbls)
			}
			deleted := map[chunks.HeadSeriesRef]labels.Labels{}
			for i := 0; i < len(created); i += 3 {
				deleted[chunks.HeadSeriesRef(w*seriesPerWriter+i+1)] = created[i]
			}
			c.DeleteSeries(deleted)
		}(w)
	}

	close(start)
	writersWG.Wait()
	close(done)
	recountWG.Wait()

	// Assert only once everything is quiescent. A reader can catch a total
	// between a stripe's swap and the matching delta, so the invariant is a
	// steady-state one; asserting it mid-flight would be flaky by design.
	requireTotalsMatchStripes(t, c)

	updateMetricsWithRing(c, idleCutoff, ringTokens, 100, 300)
	requireTotalsMatchStripes(t, c)

	// Sanity: the mutations really did move all four counters, so the assertion
	// above is not comparing zeroes.
	assert.Positive(t, c.Tracked())
	assert.Positive(t, c.Active())
	assert.Positive(t, c.Owned())
	assert.Positive(t, c.ActiveNativeHistogram())
	assert.Less(t, c.Owned(), c.Tracked(), "the ring should leave some entries unowned")
}

// TestActiveSeries_PurgeToHeadDoesNotInflateActive is the regression test for
// conflating the two cutoffs purge is given.
//
// With owned-series tracking on, purge is called after head compaction with a
// cutoff of two block ranges, which is hours old. If that same cutoff
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
	assert.Equal(t, 10, c.Owned(), "series are owned when they arrive")

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

// TestActiveSeries_DeleteSeriesReleasesExactlyTheDeletedHeadSeries covers the
// mechanism that keeps the owned count aligned with memory: the head reports the
// series it deleted, and only those entries are released.
func TestActiveSeries_DeleteSeriesReleasesExactlyTheDeletedHeadSeries(t *testing.T) {
	now := time.Now()
	ringTokens := []uint32{100, 200}

	c := NewActiveSeries()

	gone := labels.FromStrings("__name__", "gone")
	kept := labels.FromStrings("__name__", "kept")
	goneUnowned := labels.FromStrings("__name__", "gone_unowned")
	c.UpdateSeriesWithRef(gone, gone.Hash(), 11, knownSeriesToken(50), now, true, copyFn)
	c.UpdateSeriesWithRef(kept, kept.Hash(), 12, knownSeriesToken(50), now, false, copyFn)
	c.UpdateSeriesWithRef(goneUnowned, goneUnowned.Hash(), 13, knownSeriesToken(150), now, false, copyFn)
	updateMetricsWithRing(c, now.Add(-10*time.Minute), ringTokens, 100) // keys <100 owned, keys in (100,200] not owned
	require.Equal(t, 3, c.Tracked())
	require.Equal(t, 2, c.Owned())
	require.Equal(t, 3, c.Active())
	require.Equal(t, 1, c.ActiveNativeHistogram())

	c.DeleteSeries(map[chunks.HeadSeriesRef]labels.Labels{11: gone, 13: goneUnowned})

	assert.Equal(t, 1, c.Tracked(), "both deleted series are released")
	assert.Equal(t, 1, c.Owned(), "an owned deleted series leaves owned; an unowned one never counted")
	assert.Equal(t, 1, c.Active())
	assert.Equal(t, 0, c.ActiveNativeHistogram(), "the deleted native histogram leaves that count too")

	active, owned, nh := sumStripes(c)
	assert.Equal(t, []int{1, 1, 0}, []int{active, owned, nh}, "per-stripe counters agree with the totals")

	// A recount must agree with the incremental bookkeeping.
	updateMetricsWithRing(c, now.Add(-10*time.Minute), ringTokens, 100)
	assert.Equal(t, 1, c.Tracked())
	assert.Equal(t, 1, c.Owned())
	assert.Equal(t, 1, c.Active())
}

// TestActiveSeries_DeleteSeriesKeepsSeriesRecreatedInTheHead covers the race the
// head reference exists for. The head calls PostDeletion after releasing its lock,
// so a sample can re-create the same labels as a new head series first. That
// sample moves the entry to the new reference, and the stale deletion must not
// remove it.
func TestActiveSeries_DeleteSeriesKeepsSeriesRecreatedInTheHead(t *testing.T) {
	now := time.Now()
	c := NewActiveSeries()
	setRingState(c, []uint32{100}, 100)

	lbls := labels.FromStrings("__name__", "flapping")
	c.UpdateSeriesWithRef(lbls, lbls.Hash(), 21, knownSeriesToken(50), now, false, copyFn)

	// The head deletes ref 21; before the callback runs, a sample re-creates the
	// series as ref 35.
	c.UpdateSeriesWithRef(lbls, lbls.Hash(), 35, knownSeriesToken(50), now.Add(time.Second), false, copyFn)
	c.DeleteSeries(map[chunks.HeadSeriesRef]labels.Labels{21: lbls})

	assert.Equal(t, 1, c.Tracked(), "the series is live again under a new reference and must stay tracked")
	assert.Equal(t, 1, c.Owned())

	// When the head later deletes ref 35, it goes.
	c.DeleteSeries(map[chunks.HeadSeriesRef]labels.Labels{35: lbls})
	assert.Equal(t, 0, c.Tracked())
	assert.Equal(t, 0, c.Owned())
}

// TestActiveSeries_DeleteSeriesIgnoresUnknownReferences checks that entries
// created without a head reference, and deletions for series never tracked, are
// both no-ops rather than corrupting counts.
func TestActiveSeries_DeleteSeriesIgnoresUnknownReferences(t *testing.T) {
	now := time.Now()
	c := NewActiveSeries()

	noRef := labels.FromStrings("__name__", "no_ref")
	c.UpdateSeries(noRef, noRef.Hash(), noRingToken, now, false, copyFn)
	never := labels.FromStrings("__name__", "never_tracked")

	c.DeleteSeries(map[chunks.HeadSeriesRef]labels.Labels{7: noRef, 8: never})

	assert.Equal(t, 1, c.Tracked(), "an entry with no head reference is never released by the callback")
	assert.Equal(t, 1, c.Active())
}

// TestActiveSeries_EntryWithoutRefAdoptsOne checks that an entry first created
// without a head reference picks one up from a later sample, so the head's
// deletion callback can still release it.
func TestActiveSeries_EntryWithoutRefAdoptsOne(t *testing.T) {
	now := time.Now()
	c := NewActiveSeries()

	lbls := labels.FromStrings("__name__", "late_ref")
	c.UpdateSeries(lbls, lbls.Hash(), noRingToken, now, false, copyFn)
	c.UpdateSeriesWithRef(lbls, lbls.Hash(), 42, knownSeriesToken(noRingToken), now.Add(time.Second), false, copyFn)

	c.DeleteSeries(map[chunks.HeadSeriesRef]labels.Labels{42: lbls})
	assert.Equal(t, 0, c.Tracked(), "the adopted reference lets the callback release the entry")
}

// TestActiveSeries_NewSeriesIsOwnedWhateverTheRing covers a series reaching this
// instance in a token range its own view of the ring says it does not own, as
// happens when it has gained ranges the distributor already routes to it and has
// not yet seen the ring change. The series arrived because it was routed here, so
// it must count as owned, or it would escape the limits.
func TestActiveSeries_NewSeriesIsOwnedWhateverTheRing(t *testing.T) {
	c := NewActiveSeries()
	now := time.Now()
	ringTokens := []uint32{100, 200}
	updateMetricsWithRing(c, now.Add(-time.Minute), ringTokens) // owns nothing

	lbls := labels.FromStrings("__name__", "metric")
	c.UpdateSeries(lbls, lbls.Hash(), 50, now, false, copyFn)
	assert.Equal(t, 1, c.Owned(), "a series which arrived here counts as owned")

	// Until the ring changes, the recount keeps it.
	updateMetricsWithRing(c, now.Add(-time.Minute), ringTokens)
	assert.Equal(t, 1, c.Owned())

	// A ring change re-evaluates it.
	updateMetricsWithRing(c, now.Add(-time.Minute), ringTokens, 200)
	assert.Equal(t, 0, c.Owned())
}

// TestActiveSeries_TokenIsComputedOnlyForNewSeries covers how the tracker gets a
// series' ring token: computed from its labels as the distributor does, once, when
// the series is new.
func TestActiveSeries_TokenIsComputedOnlyForNewSeries(t *testing.T) {
	lbls := labels.FromStrings("__name__", "metric", "job", "a")
	adapters := cortexpb.FromLabelsToLabelAdapters(lbls)
	want, err := ring.TokenForLabels("tenant", adapters, true)
	require.NoError(t, err)

	c := NewActiveSeries()
	now := time.Now()
	c.UpdateSeriesWithRef(lbls, lbls.Hash(), 1, seriesToken{userID: "tenant", labels: adapters, shardByAllLabels: true}, now, false, copyFn)

	stripe := &c.stripes[lbls.Hash()%numActiveSeriesStripes]
	stripe.mu.RLock()
	entry := stripe.refs[lbls.Hash()][0]
	stripe.mu.RUnlock()
	assert.Equal(t, want, entry.key)
	assert.False(t, entry.keyless)

	t.Run("zero value has no token", func(t *testing.T) {
		_, ok := seriesToken{}.resolve()
		assert.False(t, ok)
	})
	t.Run("a known token is used as is", func(t *testing.T) {
		key, ok := knownSeriesToken(42).resolve()
		assert.True(t, ok)
		assert.Equal(t, uint32(42), key)
	})
}

// TestActiveSeries_SeriesWithoutTokenStaysOwned covers a series whose ring token
// cannot be computed: sharding by metric name, for a series with no metric name.
// It is tracked, counts as owned, and stays owned across ring changes, since its
// ownership cannot be evaluated.
func TestActiveSeries_SeriesWithoutTokenStaysOwned(t *testing.T) {
	lbls := labels.FromStrings("job", "no-metric-name")
	token := seriesToken{userID: "tenant", labels: cortexpb.FromLabelsToLabelAdapters(lbls), shardByAllLabels: false}
	_, ok := token.resolve()
	require.False(t, ok, "the token cannot be computed without a metric name")

	c := NewActiveSeries()
	now := time.Now()
	c.UpdateSeriesWithRef(lbls, lbls.Hash(), 1, token, now, false, copyFn)
	require.Equal(t, 1, c.Tracked())
	require.Equal(t, 1, c.Owned())

	updateMetricsWithRing(c, now.Add(-time.Minute), []uint32{100, 200}) // owns nothing
	assert.Equal(t, 1, c.Owned(), "a series whose ownership cannot be evaluated stays owned")
}

// TestActiveSeries_SeriesIsNotJudgedByAnOlderViewOfTheRing covers the rule that a
// series is only judged by a view of the ring read after it arrived. A series
// created at read 5 may have been routed here by a ring newer than this ingester's
// read 5 view, so that view must not un-count it, even when ownership is applied
// for the first time, or changes. A view from a later read judges it.
func TestActiveSeries_SeriesIsNotJudgedByAnOlderViewOfTheRing(t *testing.T) {
	now := time.Now()
	ringTokens := []uint32{100, 200}
	ownsNothing := ownedPositionsFor(ringTokens)
	ownsNothingFingerprint := testRingFingerprint(ringTokens, ownsNothing)

	c := NewActiveSeries()
	old := labels.FromStrings("__name__", "old")
	c.UpdateSeriesWithRef(old, old.Hash(), 1, seriesToken{key: 50, known: true, ringRead: 4}, now, false, copyFn)
	recent := labels.FromStrings("__name__", "recent")
	c.UpdateSeriesWithRef(recent, recent.Hash(), 2, seriesToken{key: 50, known: true, ringRead: 5}, now, false, copyFn)

	// Ownership applied as of read 5: the old series is judged, the recent one not.
	c.UpdateMetrics(now.Add(-time.Minute), ringTokens, ownsNothing, ownsNothingFingerprint, 5)
	require.Equal(t, 1, c.Owned(), "only the series created before the view was read is judged")

	// The same view again judges nothing new.
	c.UpdateMetrics(now.Add(-time.Minute), ringTokens, ownsNothing, ownsNothingFingerprint, 5)
	require.Equal(t, 1, c.Owned())

	// A later read with the same ownership judges the series it was too new for.
	c.UpdateMetrics(now.Add(-time.Minute), ringTokens, ownsNothing, ownsNothingFingerprint, 6)
	require.Equal(t, 0, c.Owned())
}

// TestActiveSeries_NewerViewDoesNotRejudgeSettledSeries checks that a newer read
// with unchanged ownership only judges the entries that were waiting for it, so
// that a heartbeat does not re-evaluate every series.
func TestActiveSeries_NewerViewDoesNotRejudgeSettledSeries(t *testing.T) {
	now := time.Now()
	ringTokens := []uint32{100, 200}
	ownsAll := ownedPositionsFor(ringTokens, 100, 200)
	ownsAllFingerprint := testRingFingerprint(ringTokens, ownsAll)

	c := NewActiveSeries()
	lbls := labels.FromStrings("__name__", "settled")
	c.UpdateSeriesWithRef(lbls, lbls.Hash(), 1, seriesToken{key: 50, known: true, ringRead: 1}, now, false, copyFn)
	c.UpdateMetrics(now.Add(-time.Minute), ringTokens, ownsAll, ownsAllFingerprint, 2)
	require.Equal(t, 1, c.Owned())

	// Corrupt the cached ownership. Only a judgement would correct it.
	stripe := &c.stripes[lbls.Hash()%numActiveSeriesStripes]
	stripe.mu.Lock()
	stripe.refs[lbls.Hash()][0].owned = false
	stripe.mu.Unlock()

	c.UpdateMetrics(now.Add(-time.Minute), ringTokens, ownsAll, ownsAllFingerprint, 3)
	require.Equal(t, 0, c.Owned(), "a settled entry is not judged again by a newer read with the same ownership")
}

// TestActiveSeries_UnknownOwnershipJudgesNothing covers a recount for which
// ownership could not be computed. It must not judge the entries waiting for a
// newer view by the bitmap the tracker already has, nor record the newer view as
// applied, since nothing was computed from it.
func TestActiveSeries_UnknownOwnershipJudgesNothing(t *testing.T) {
	now := time.Now()
	ringTokens := []uint32{100, 200}
	ownsNothing := ownedPositionsFor(ringTokens)
	fingerprint := testRingFingerprint(ringTokens, ownsNothing)

	c := NewActiveSeries()
	c.UpdateMetrics(now.Add(-time.Minute), ringTokens, ownsNothing, fingerprint, 5)
	lbls := labels.FromStrings("__name__", "waiting")
	c.UpdateSeriesWithRef(lbls, lbls.Hash(), 1, seriesToken{key: 50, known: true, ringRead: 5}, now, false, copyFn)
	require.Equal(t, 1, c.Owned())

	c.UpdateMetrics(now.Add(-time.Minute), nil, nil, 0, 6)
	require.Equal(t, 1, c.Owned(), "unknown ownership judges nothing")
	require.Equal(t, uint32(5), c.viewRead, "and records no newer view")

	c.UpdateMetrics(now.Add(-time.Minute), ringTokens, ownsNothing, fingerprint, 6)
	require.Equal(t, 0, c.Owned(), "the next known ownership judges it")
}

// TestActiveSeries_SeededSeries covers entries created from the head after a
// restart: owned until a newer view judges them, not active, and made active, with
// their kind, by their first sample.
func TestActiveSeries_SeededSeries(t *testing.T) {
	now := time.Now()
	idle := now.Add(-time.Hour).UnixNano()
	ringTokens := []uint32{100, 200}
	ownsToken100 := ownedPositionsFor(ringTokens, 100)
	fingerprint := testRingFingerprint(ringTokens, ownsToken100)

	c := NewActiveSeries()
	owned := labels.FromStrings("__name__", "owned")
	notOwned := labels.FromStrings("__name__", "not_owned")
	c.SeedSeries(owned, owned.Hash(), 1, seriesToken{key: 50, known: true, ringRead: 1}, idle)
	c.SeedSeries(notOwned, notOwned.Hash(), 2, seriesToken{key: 150, known: true, ringRead: 1}, idle)
	c.SeedSeries(owned, owned.Hash(), 1, seriesToken{key: 50, known: true, ringRead: 1}, idle) // already tracked
	require.Equal(t, 2, c.Tracked())
	require.Equal(t, 2, c.Owned())
	require.Equal(t, 0, c.Active(), "seeded series have received no sample")

	c.UpdateMetrics(now.Add(-time.Minute), ringTokens, ownsToken100, fingerprint, 2)
	require.Equal(t, 1, c.Owned(), "a view read after seeding judges them")
	require.Equal(t, 0, c.Active())

	c.UpdateSeries(owned, owned.Hash(), 50, now, true, copyFn)
	require.Equal(t, 1, c.Active(), "the first sample makes it active")
	require.Equal(t, 1, c.ActiveNativeHistogram(), "and sets its kind")
	c.UpdateSeries(owned, owned.Hash(), 50, now.Add(time.Second), true, copyFn)
	require.Equal(t, 1, c.Active(), "only once")

	c.DeleteSeries(map[chunks.HeadSeriesRef]labels.Labels{2: notOwned})
	require.Equal(t, 1, c.Tracked())
	require.Equal(t, 1, c.Active(), "deleting a seeded series that never became active leaves active alone")
}

// TestActiveSeries_ReactivationTakesTheSamplesKind covers a series which switches
// between native histogram and float samples. Without retention its entry would be
// released once idle and re-created by the next sample, with that sample's kind,
// so a retained entry must take the kind of the sample that reactivates it.
func TestActiveSeries_ReactivationTakesTheSamplesKind(t *testing.T) {
	now := time.Now()
	idle := 10 * time.Minute
	ringTokens := []uint32{100}

	c := NewActiveSeries()
	lbls := labels.FromStrings("__name__", "switches")
	c.UpdateSeries(lbls, lbls.Hash(), 50, now, true, copyFn)
	updateMetricsWithRing(c, now.Add(-idle), ringTokens, 100)
	require.Equal(t, 1, c.ActiveNativeHistogram())

	// Idle, then a float sample.
	later := now.Add(idle + time.Minute)
	updateMetricsWithRing(c, later.Add(-idle), ringTokens, 100)
	require.Equal(t, 0, c.Active())
	c.UpdateSeries(lbls, lbls.Hash(), 50, later, false, copyFn)

	require.Equal(t, 1, c.Active())
	require.Equal(t, 0, c.ActiveNativeHistogram(), "the reactivating sample is a float")
	updateMetricsWithRing(c, later.Add(-idle), ringTokens, 100)
	require.Equal(t, 0, c.ActiveNativeHistogram(), "and the recount keeps it so")
}
