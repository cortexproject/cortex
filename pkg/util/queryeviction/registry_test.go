package queryeviction

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	querier_stats "github.com/cortexproject/cortex/pkg/querier/stats"
)

// newTestStats creates a QueryStats with the given fetched samples value.
func newTestStats(fetchedSamples uint64) *querier_stats.QueryStats {
	stats := &querier_stats.QueryStats{}
	stats.AddFetchedSamples(fetchedSamples)
	return stats
}

// testMetricFunc is a MetricFunc that returns FetchedSamples for testing.
func testMetricFunc(s *querier_stats.QueryStats) uint64 {
	return s.LoadFetchedSamples()
}

func TestRegister_UniqueMonotonicallyIncreasingIDs(t *testing.T) {
	reg := NewQueryRegistry(testMetricFunc)
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	ids := make([]uint64, 10)
	for i := range 10 {
		ids[i] = reg.Register(cancel, newTestStats(0), "query", "user", "")
	}

	for i := 1; i < len(ids); i++ {
		assert.Greater(t, ids[i], ids[i-1], "IDs should be monotonically increasing")
	}

	// Verify all IDs are unique.
	seen := make(map[uint64]bool)
	for _, id := range ids {
		assert.False(t, seen[id], "ID %d should be unique", id)
		seen[id] = true
	}
}

func TestDeregister_RemovesEntry(t *testing.T) {
	reg := NewQueryRegistry(testMetricFunc)
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	id := reg.Register(cancel, newTestStats(100), "query", "user", "")
	require.Equal(t, 1, reg.Len())

	// FindHeaviest should return the registered entry.
	results := reg.FindHeaviest(1, 0)
	require.Len(t, results, 1)
	assert.Equal(t, id, results[0].QueryID)

	// Deregister and verify it's gone.
	reg.Deregister(id)
	assert.Equal(t, 0, reg.Len())
	assert.Nil(t, reg.FindHeaviest(1, 0), "FindHeaviest should return nil after deregistering the only entry")
}

func TestDeregister_UnknownID_IsNoOp(t *testing.T) {
	reg := NewQueryRegistry(testMetricFunc)
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	id := reg.Register(cancel, newTestStats(100), "query", "user", "")

	// Deregister an ID that was never registered.
	reg.Deregister(99999)

	// Original entry should still be present.
	assert.Equal(t, 1, reg.Len())
	results := reg.FindHeaviest(1, 0)
	require.Len(t, results, 1)
	assert.Equal(t, id, results[0].QueryID)
}

func TestFindHeaviest_ReturnsHighestMetricValue(t *testing.T) {
	reg := NewQueryRegistry(testMetricFunc)
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg.Register(cancel, newTestStats(100), "small-query", "user1", "")
	reg.Register(cancel, newTestStats(500), "medium-query", "user2", "")
	heaviestID := reg.Register(cancel, newTestStats(1000), "large-query", "user3", "")
	reg.Register(cancel, newTestStats(200), "another-query", "user4", "")

	results := reg.FindHeaviest(1, 0)
	require.Len(t, results, 1)
	assert.Equal(t, heaviestID, results[0].QueryID)
	assert.Equal(t, "large-query", results[0].QueryExpr)
	assert.Equal(t, uint64(1000), results[0].Stats.LoadFetchedSamples())
}

func TestFindHeaviest_EmptyRegistry(t *testing.T) {
	reg := NewQueryRegistry(testMetricFunc)
	assert.Nil(t, reg.FindHeaviest(1, 0), "FindHeaviest should return nil for empty registry")
}

func TestLen_ReflectsCurrentCount(t *testing.T) {
	reg := NewQueryRegistry(testMetricFunc)
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	assert.Equal(t, 0, reg.Len())

	id1 := reg.Register(cancel, newTestStats(10), "q1", "u1", "")
	assert.Equal(t, 1, reg.Len())

	id2 := reg.Register(cancel, newTestStats(20), "q2", "u2", "")
	assert.Equal(t, 2, reg.Len())

	id3 := reg.Register(cancel, newTestStats(30), "q3", "u3", "")
	assert.Equal(t, 3, reg.Len())

	reg.Deregister(id2)
	assert.Equal(t, 2, reg.Len())

	reg.Deregister(id1)
	assert.Equal(t, 1, reg.Len())

	reg.Deregister(id3)
	assert.Equal(t, 0, reg.Len())
}

func TestConcurrent_RegisterDeregisterFindHeaviest(t *testing.T) {
	reg := NewQueryRegistry(testMetricFunc)

	const goroutines = 20
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			for i := range opsPerGoroutine {
				_, cancel := context.WithCancel(context.Background())
				stats := newTestStats(uint64(i))
				id := reg.Register(cancel, stats, "concurrent-query", "user", "")

				// Interleave FindHeaviest and Len calls.
				_ = reg.FindHeaviest(1, 0)
				_ = reg.Len()

				reg.Deregister(id)
				cancel()
			}
		}()
	}

	wg.Wait()

	// After all goroutines complete, registry should be empty.
	assert.Equal(t, 0, reg.Len())
}

func TestResolveMetricFunc_AllSupportedMetrics(t *testing.T) {
	stats := &querier_stats.QueryStats{}
	stats.AddFetchedSamples(20)
	stats.AddFetchedSeries(30)
	stats.AddFetchedChunks(40)
	stats.AddFetchedChunkBytes(50)

	tests := []struct {
		name          string
		metricName    string
		expectedValue uint64
	}{
		{name: "fetched_samples", metricName: "fetched_samples", expectedValue: 20},
		{name: "empty string defaults to fetched_samples", metricName: "", expectedValue: 20},
		{name: "fetched_series", metricName: "fetched_series", expectedValue: 30},
		{name: "fetched_chunks", metricName: "fetched_chunks", expectedValue: 40},
		{name: "fetched_chunk_bytes", metricName: "fetched_chunk_bytes", expectedValue: 50},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fn, err := ResolveMetricFunc(tc.metricName)
			require.NoError(t, err)
			require.NotNil(t, fn)
			assert.Equal(t, tc.expectedValue, fn(stats))
		})
	}
}

func TestResolveMetricFunc_UnsupportedMetric(t *testing.T) {
	fn, err := ResolveMetricFunc("unknown_metric")
	assert.Error(t, err)
	assert.Nil(t, fn)
	assert.Contains(t, err.Error(), "unsupported eviction metric")
}
