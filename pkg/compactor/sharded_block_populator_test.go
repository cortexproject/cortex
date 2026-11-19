package compactor

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

// testSample is a float-only chunks.Sample; the package's own implementation is
// unexported.
type testSample struct {
	t int64
	v float64
}

func (s testSample) T() int64                      { return s.t }
func (s testSample) F() float64                    { return s.v }
func (s testSample) H() *histogram.Histogram       { return nil }
func (s testSample) FH() *histogram.FloatHistogram { return nil }
func (s testSample) Type() chunkenc.ValueType      { return chunkenc.ValFloat }
func (s testSample) Copy() chunks.Sample           { return s }

// sliceChunkSeriesSet serves a pre-sorted slice of series once.
type sliceChunkSeriesSet struct {
	series []storage.ChunkSeries
	idx    int
}

func newSliceChunkSeriesSet(series []storage.ChunkSeries) *sliceChunkSeriesSet {
	return &sliceChunkSeriesSet{series: series, idx: -1}
}

func (s *sliceChunkSeriesSet) Next() bool {
	s.idx++
	return s.idx < len(s.series)
}
func (s *sliceChunkSeriesSet) At() storage.ChunkSeries           { return s.series[s.idx] }
func (s *sliceChunkSeriesSet) Err() error                        { return nil }
func (s *sliceChunkSeriesSet) Warnings() annotations.Annotations { return nil }

type collectedSeries struct {
	labels  string
	samples []testSample
}

// collect flattens samples across chunk boundaries on purpose. A merge tree may
// lay the same samples out in different chunks than a flat merge, which changes
// the output block's chunk count but not its data; the samples are what must
// match.
func collect(t *testing.T, set storage.ChunkSeriesSet) []collectedSeries {
	t.Helper()
	var out []collectedSeries
	for set.Next() {
		s := set.At()
		cur := collectedSeries{labels: s.Labels().String()}
		chkIter := s.Iterator(nil)
		for chkIter.Next() {
			var valIter chunkenc.Iterator
			valIter = chkIter.At().Chunk.Iterator(valIter)
			for valIter.Next() == chunkenc.ValFloat {
				ts, v := valIter.At()
				cur.samples = append(cur.samples, testSample{t: ts, v: v})
			}
			require.NoError(t, valIter.Err())
		}
		require.NoError(t, chkIter.Err())
		out = append(out, cur)
	}
	require.NoError(t, set.Err())
	return out
}

// buildSets returns setCount sorted sets. Each series is replicated into three
// sets with partially overlapping sample windows, and a given timestamp always
// carries the same value, which is how ingester replicas present the same
// series to the compactor.
func buildSets(setCount, seriesCount int) []storage.ChunkSeriesSet {
	perSet := make([][]storage.ChunkSeries, setCount)
	for i := range seriesCount {
		lbls := labels.FromStrings(labels.MetricName, "test_metric", "series", fmt.Sprintf("%03d", i))
		for _, offset := range []int{0, 1, 3} {
			set := (i + offset) % setCount
			samples := make([]chunks.Sample, 0, 12)
			start := int64(offset) * 40
			for k := range 12 {
				ts := start + int64(k)*10
				samples = append(samples, testSample{t: ts, v: float64(ts) * 1.5})
			}
			perSet[set] = append(perSet[set], storage.NewListChunkSeriesFromSamples(lbls, samples))
		}
	}

	sets := make([]storage.ChunkSeriesSet, 0, setCount)
	for _, series := range perSet {
		sort.Slice(series, func(a, b int) bool {
			return labels.Compare(series[a].Labels(), series[b].Labels()) < 0
		})
		sets = append(sets, newSliceChunkSeriesSet(series))
	}
	return sets
}

func TestMergeSubtreesMatchesFlatMerge(t *testing.T) {
	const (
		setCount    = 9
		seriesCount = 40
	)
	mergeFunc := storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge)

	flat := collect(t, storage.NewMergeChunkSeriesSet(buildSets(setCount, seriesCount), 0, mergeFunc))
	require.Len(t, flat, seriesCount)

	// Fan-ins that divide the set count evenly and that do not, so the short
	// final group is covered too.
	for _, fanIn := range []int{2, 3, 4, 8} {
		t.Run(fmt.Sprintf("fan_in_%d", fanIn), func(t *testing.T) {
			c := ShardedBlockPopulator{blockMergeFanIn: fanIn, logger: log.NewNopLogger()}
			sets := buildSets(setCount, seriesCount)
			roots := c.mergeSubtrees(context.Background(), sets, mergeFunc)
			require.Less(t, len(roots), len(sets), "fan-in should have grouped the sets")

			tree := collect(t, storage.NewMergeChunkSeriesSet(roots, 0, mergeFunc))
			require.Equal(t, flat, tree)
		})
	}
}

func TestMergeSubtreesDisabled(t *testing.T) {
	mergeFunc := storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge)
	for _, fanIn := range []int{0, 1, -1} {
		t.Run(fmt.Sprintf("fan_in_%d", fanIn), func(t *testing.T) {
			c := ShardedBlockPopulator{blockMergeFanIn: fanIn, logger: log.NewNopLogger()}
			sets := buildSets(4, 4)
			require.Equal(t, sets, c.mergeSubtrees(context.Background(), sets, mergeFunc),
				"a disabled fan-in must return the sets untouched")
		})
	}
}

func TestMergeSubtreesGrouping(t *testing.T) {
	mergeFunc := storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge)
	for _, tc := range []struct {
		sets, fanIn, expected int
	}{
		{sets: 10, fanIn: 5, expected: 2},
		{sets: 10, fanIn: 3, expected: 4},
		{sets: 10, fanIn: 10, expected: 10}, // not larger than fan-in, left flat
		{sets: 10, fanIn: 11, expected: 10},
		{sets: 2761, fanIn: 64, expected: 44},
	} {
		t.Run(fmt.Sprintf("sets_%d_fan_in_%d", tc.sets, tc.fanIn), func(t *testing.T) {
			c := ShardedBlockPopulator{blockMergeFanIn: tc.fanIn, logger: log.NewNopLogger()}
			sets := make([]storage.ChunkSeriesSet, tc.sets)
			for i := range sets {
				sets[i] = newSliceChunkSeriesSet(nil)
			}
			require.Len(t, c.mergeSubtrees(context.Background(), sets, mergeFunc), tc.expected)
		})
	}
}

// latencySet models the index read that backs a merge advance. Advancing a
// real BlockChunkSeriesSet reads the winning block's series entry off disk, and
// that read is what a flat merge serialises onto one goroutine.
type latencySet struct {
	storage.ChunkSeriesSet
	latency  time.Duration
	advances *atomic.Int64
}

func (l *latencySet) Next() bool {
	l.advances.Add(1)
	if l.latency > 0 {
		time.Sleep(l.latency)
	}
	return l.ChunkSeriesSet.Next()
}

func buildLatencySets(setCount, seriesCount int, latency time.Duration, advances *atomic.Int64) []storage.ChunkSeriesSet {
	sets := buildSets(setCount, seriesCount)
	for i, s := range sets {
		sets[i] = &latencySet{ChunkSeriesSet: s, latency: latency, advances: advances}
	}
	return sets
}

// BenchmarkMergeFanIn compares the flat merge against the subtree merge.
//
// latency=0 isolates the cost of the extra goroutines and channel handoffs,
// where the flat merge is expected to win. The non-zero cases are the ones the
// flag exists for: with per-advance read latency the flat merge serialises
// every read onto one goroutine, and grouping overlaps them.
//
// Latencies below roughly 100us are not worth running: time.Sleep cannot
// resolve them, so every row collapses onto the timer floor and the comparison
// says nothing. 1ms is close to a real EBS gp3 read.
//
// Run with a fixed iteration count for stable numbers, and do not pin to one
// CPU, which inflates every sleep. The latency cases are slow by construction:
//
//	go test -run XXX -bench BenchmarkMergeFanIn -benchtime 5x ./pkg/compactor/
func BenchmarkMergeFanIn(b *testing.B) {
	const (
		setCount    = 64
		seriesCount = 1024
	)
	mergeFunc := storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge)

	for _, latency := range []time.Duration{0, time.Millisecond} {
		for _, fanIn := range []int{0, 4, 8, 16} {
			name := fmt.Sprintf("latency=%v/fan_in=%d", latency, fanIn)
			b.Run(name, func(b *testing.B) {
				c := ShardedBlockPopulator{blockMergeFanIn: fanIn, logger: log.NewNopLogger()}
				var advances atomic.Int64
				// Goroutines that advance sets, which is the read concurrency
				// on offer. A flat merge has one however many sets it holds.
				mergeGoroutines := 1
				if fanIn >= 2 && setCount > fanIn {
					mergeGoroutines = (setCount + fanIn - 1) / fanIn
				}
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					sets := buildLatencySets(setCount, seriesCount, latency, &advances)
					ctx, cancel := context.WithCancel(context.Background())
					b.StartTimer()

					// Mirrors PopulateBlock: the root is always wrapped, only
					// the layer underneath it changes.
					set := NewBackgroundChunkSeriesSet(ctx, storage.NewMergeChunkSeriesSet(
						c.mergeSubtrees(ctx, sets, mergeFunc), 0, mergeFunc))

					got := 0
					for set.Next() {
						chkIter := set.At().Iterator(nil)
						for chkIter.Next() {
						}
						if err := chkIter.Err(); err != nil {
							b.Fatal(err)
						}
						got++
					}
					if got != seriesCount {
						b.Fatalf("got %d series, want %d", got, seriesCount)
					}
					cancel()
				}
				// ns/advance is the latency actually achieved per set advance,
				// which is the guard against trusting a row whose sleeps never
				// resolved to the requested latency.
				b.ReportMetric(float64(mergeGoroutines), "merge_goroutines")
				n := advances.Load()
				if n > 0 && b.N > 0 {
					b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(n), "ns/advance")
					b.ReportMetric(float64(n)/float64(b.N), "advances/op")
				}
			})
		}
	}
}
