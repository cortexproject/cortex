package stats

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStats_WallTime(t *testing.T) {
	t.Run("add and load wall time", func(t *testing.T) {
		stats, _ := ContextWithEmptyStats(context.Background())
		stats.AddWallTime(time.Second)
		stats.AddWallTime(time.Second)

		assert.Equal(t, 2*time.Second, stats.LoadWallTime())
	})

	t.Run("add and load wall time nil receiver", func(t *testing.T) {
		var stats *QueryStats
		stats.AddWallTime(time.Second)

		assert.Equal(t, time.Duration(0), stats.LoadWallTime())
	})
}

func TestStats_AddFetchedSeries(t *testing.T) {
	t.Parallel()
	t.Run("add and load series", func(t *testing.T) {
		stats, _ := ContextWithEmptyStats(context.Background())
		stats.AddFetchedSeries(100)
		stats.AddFetchedSeries(50)

		assert.Equal(t, uint64(150), stats.LoadFetchedSeries())
	})

	t.Run("add and load series nil receiver", func(t *testing.T) {
		var stats *QueryStats
		stats.AddFetchedSeries(50)

		assert.Equal(t, uint64(0), stats.LoadFetchedSeries())
	})
}

func TestQueryStats_AddExtraFields(t *testing.T) {
	t.Parallel()
	t.Run("add and load extra fields", func(t *testing.T) {
		stats, _ := ContextWithEmptyStats(context.Background())
		stats.AddExtraFields("a", "b")
		stats.AddExtraFields("c")

		checkExtraFields(t, []interface{}{"a", "b", "c", ""}, stats.LoadExtraFields())
	})

	t.Run("add and load extra fields nil receiver", func(t *testing.T) {
		var stats *QueryStats
		stats.AddExtraFields("a", "b")

		checkExtraFields(t, []interface{}{}, stats.LoadExtraFields())
	})
}

func TestStats_AddFetchedChunkBytes(t *testing.T) {
	t.Parallel()
	t.Run("add and load bytes", func(t *testing.T) {
		stats, _ := ContextWithEmptyStats(context.Background())
		stats.AddFetchedChunkBytes(4096)
		stats.AddFetchedChunkBytes(4096)

		assert.Equal(t, uint64(8192), stats.LoadFetchedChunkBytes())
	})

	t.Run("add and load bytes nil receiver", func(t *testing.T) {
		var stats *QueryStats
		stats.AddFetchedChunkBytes(1024)

		assert.Equal(t, uint64(0), stats.LoadFetchedChunkBytes())
	})
}

func TestStats_AddFetchedDataBytes(t *testing.T) {
	t.Parallel()
	t.Run("add and load bytes", func(t *testing.T) {
		stats, _ := ContextWithEmptyStats(context.Background())
		stats.AddFetchedDataBytes(4096)
		stats.AddFetchedDataBytes(4096)

		assert.Equal(t, uint64(8192), stats.LoadFetchedDataBytes())
	})

	t.Run("add and load bytes nil receiver", func(t *testing.T) {
		var stats *QueryStats
		stats.AddFetchedDataBytes(1024)

		assert.Equal(t, uint64(0), stats.LoadFetchedDataBytes())
	})
}

func TestStats_Merge(t *testing.T) {
	t.Parallel()
	t.Run("merge two stats objects", func(t *testing.T) {
		stats1 := &QueryStats{}
		stats1.AddWallTime(time.Millisecond)
		stats1.AddFetchedSeries(50)
		stats1.AddFetchedChunkBytes(42)
		stats1.AddFetchedDataBytes(100)
		stats1.AddExtraFields("a", "b")
		stats1.AddExtraFields("a", "b")

		stats2 := &QueryStats{}
		stats2.AddWallTime(time.Second)
		stats2.AddFetchedSeries(60)
		stats2.AddFetchedChunkBytes(100)
		stats2.AddFetchedDataBytes(101)
		stats2.AddExtraFields("c", "d")

		stats1.Merge(stats2)

		assert.Equal(t, 1001*time.Millisecond, stats1.LoadWallTime())
		assert.Equal(t, uint64(110), stats1.LoadFetchedSeries())
		assert.Equal(t, uint64(142), stats1.LoadFetchedChunkBytes())
		assert.Equal(t, uint64(201), stats1.LoadFetchedDataBytes())
		checkExtraFields(t, []interface{}{"a", "b", "c", "d"}, stats1.LoadExtraFields())
	})

	t.Run("merge two nil stats objects", func(t *testing.T) {
		var stats1 *QueryStats
		var stats2 *QueryStats

		stats1.Merge(stats2)

		assert.Equal(t, time.Duration(0), stats1.LoadWallTime())
		assert.Equal(t, uint64(0), stats1.LoadFetchedSeries())
		assert.Equal(t, uint64(0), stats1.LoadFetchedChunkBytes())
		assert.Equal(t, uint64(0), stats1.LoadFetchedDataBytes())
		checkExtraFields(t, []interface{}{}, stats1.LoadExtraFields())
	})
}

func checkExtraFields(t *testing.T, expected, actual []interface{}) {
	t.Parallel()
	assert.Equal(t, len(expected), len(actual))
	expectedMap := map[string]string{}
	actualMap := map[string]string{}

	for i := 0; i < len(expected); i += 2 {
		expectedMap[expected[i].(string)] = expected[i+1].(string)
		actualMap[actual[i].(string)] = actual[i+1].(string)
	}

	assert.Equal(t, expectedMap, actualMap)
}
