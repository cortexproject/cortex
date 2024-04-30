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

func TestStats_AddFetchedChunks(t *testing.T) {
	t.Parallel()
	t.Run("add and load chunks", func(t *testing.T) {
		stats, _ := ContextWithEmptyStats(context.Background())
		stats.AddFetchedChunks(4096)
		stats.AddFetchedChunks(4096)

		assert.Equal(t, uint64(8192), stats.LoadFetchedChunks())
	})

	t.Run("add and load chunks nil receiver", func(t *testing.T) {
		var stats *QueryStats
		stats.AddFetchedChunks(1024)

		assert.Equal(t, uint64(0), stats.LoadFetchedChunks())
	})
}

func TestStats_AddFetchedSamples(t *testing.T) {
	t.Parallel()
	t.Run("add and load samples", func(t *testing.T) {
		stats, _ := ContextWithEmptyStats(context.Background())
		stats.AddFetchedSamples(4096)
		stats.AddFetchedSamples(4096)

		assert.Equal(t, uint64(8192), stats.LoadFetchedSamples())
	})

	t.Run("add and load samples nil receiver", func(t *testing.T) {
		var stats *QueryStats
		stats.AddFetchedSamples(1024)

		assert.Equal(t, uint64(0), stats.LoadFetchedSamples())
	})
}

func TestStats_AddStoreGatewayTouchedPostings(t *testing.T) {
	t.Parallel()
	t.Run("add and load touched postings", func(t *testing.T) {
		stats, _ := ContextWithEmptyStats(context.Background())
		stats.AddStoreGatewayTouchedPostings(4096)
		stats.AddStoreGatewayTouchedPostings(4096)

		assert.Equal(t, uint64(8192), stats.LoadStoreGatewayTouchedPostings())
	})

	t.Run("add and load touched postings nil receiver", func(t *testing.T) {
		var stats *QueryStats
		stats.AddStoreGatewayTouchedPostings(4096)

		assert.Equal(t, uint64(0), stats.LoadStoreGatewayTouchedPostings())
	})
}

func TestStats_AddStoreGatewayTouchedPostingBytes(t *testing.T) {
	t.Parallel()
	t.Run("add and load touched postings bytes", func(t *testing.T) {
		stats, _ := ContextWithEmptyStats(context.Background())
		stats.AddStoreGatewayTouchedPostingBytes(4096)
		stats.AddStoreGatewayTouchedPostingBytes(4096)

		assert.Equal(t, uint64(8192), stats.LoadStoreGatewayTouchedPostingBytes())
	})

	t.Run("add and load touched posting bytes nil receiver", func(t *testing.T) {
		var stats *QueryStats
		stats.AddStoreGatewayTouchedPostingBytes(4096)

		assert.Equal(t, uint64(0), stats.LoadStoreGatewayTouchedPostingBytes())
	})
}

func TestStats_StorageWallTime(t *testing.T) {
	t.Run("add and load query storage wall time", func(t *testing.T) {
		stats, _ := ContextWithEmptyStats(context.Background())
		stats.AddQueryStorageWallTime(time.Second)
		stats.AddQueryStorageWallTime(time.Second)

		assert.Equal(t, 2*time.Second, stats.LoadQueryStorageWallTime())
	})

	t.Run("add and load query storage wall time nil receiver", func(t *testing.T) {
		var stats *QueryStats
		stats.AddQueryStorageWallTime(time.Second)

		assert.Equal(t, time.Duration(0), stats.LoadQueryStorageWallTime())
	})
}

func TestStats_Merge(t *testing.T) {
	t.Parallel()
	t.Run("merge two stats objects", func(t *testing.T) {
		stats1 := &QueryStats{}
		stats1.AddWallTime(time.Millisecond)
		stats1.AddQueryStorageWallTime(2 * time.Second)
		stats1.AddFetchedSeries(50)
		stats1.AddFetchedChunkBytes(42)
		stats1.AddFetchedDataBytes(100)
		stats1.AddStoreGatewayTouchedPostings(200)
		stats1.AddStoreGatewayTouchedPostingBytes(300)
		stats1.AddFetchedChunks(105)
		stats1.AddFetchedSamples(109)
		stats1.AddExtraFields("a", "b")
		stats1.AddExtraFields("a", "b")

		stats2 := &QueryStats{}
		stats2.AddWallTime(time.Second)
		stats2.AddQueryStorageWallTime(3 * time.Second)
		stats2.AddFetchedSeries(60)
		stats2.AddFetchedChunkBytes(100)
		stats2.AddFetchedDataBytes(101)
		stats1.AddStoreGatewayTouchedPostings(201)
		stats1.AddStoreGatewayTouchedPostingBytes(301)
		stats2.AddFetchedChunks(102)
		stats2.AddFetchedSamples(103)
		stats2.AddExtraFields("c", "d")

		stats1.Merge(stats2)

		assert.Equal(t, 1001*time.Millisecond, stats1.LoadWallTime())
		assert.Equal(t, 5*time.Second, stats1.LoadQueryStorageWallTime())
		assert.Equal(t, uint64(110), stats1.LoadFetchedSeries())
		assert.Equal(t, uint64(142), stats1.LoadFetchedChunkBytes())
		assert.Equal(t, uint64(201), stats1.LoadFetchedDataBytes())
		assert.Equal(t, uint64(207), stats1.LoadFetchedChunks())
		assert.Equal(t, uint64(212), stats1.LoadFetchedSamples())
		assert.Equal(t, uint64(401), stats1.LoadStoreGatewayTouchedPostings())
		assert.Equal(t, uint64(601), stats1.LoadStoreGatewayTouchedPostingBytes())
		checkExtraFields(t, []interface{}{"a", "b", "c", "d"}, stats1.LoadExtraFields())
	})

	t.Run("merge two nil stats objects", func(t *testing.T) {
		var stats1 *QueryStats
		var stats2 *QueryStats

		stats1.Merge(stats2)

		assert.Equal(t, time.Duration(0), stats1.LoadWallTime())
		assert.Equal(t, time.Duration(0), stats1.LoadQueryStorageWallTime())
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
