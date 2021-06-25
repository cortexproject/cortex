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
		var stats *Stats
		stats.AddWallTime(time.Second)

		assert.Equal(t, time.Duration(0), stats.LoadWallTime())
	})
}

func TestStats_Series(t *testing.T) {
	t.Run("add and load series", func(t *testing.T) {
		stats, _ := ContextWithEmptyStats(context.Background())
		stats.AddSeries(100)
		stats.AddSeries(50)

		assert.Equal(t, uint64(150), stats.LoadSeries())
	})

	t.Run("add and load series nil receiver", func(t *testing.T) {
		var stats *Stats
		stats.AddSeries(50)

		assert.Equal(t, uint64(0), stats.LoadSeries())
	})
}

func TestStats_Bytes(t *testing.T) {
	t.Run("add and load bytes", func(t *testing.T) {
		stats, _ := ContextWithEmptyStats(context.Background())
		stats.AddBytes(4096)
		stats.AddBytes(4096)

		assert.Equal(t, uint64(8192), stats.LoadBytes())
	})

	t.Run("add and load bytes nil receiver", func(t *testing.T) {
		var stats *Stats
		stats.AddBytes(1024)

		assert.Equal(t, uint64(0), stats.LoadBytes())
	})
}

func TestStats_Merge(t *testing.T) {
	t.Run("merge two stats objects", func(t *testing.T) {
		stats1 := &Stats{}
		stats1.AddWallTime(time.Millisecond)
		stats1.AddSeries(50)
		stats1.AddBytes(42)

		stats2 := &Stats{}
		stats2.AddWallTime(time.Second)
		stats2.AddSeries(60)
		stats2.AddBytes(100)

		stats1.Merge(stats2)

		assert.Equal(t, 1001*time.Millisecond, stats1.LoadWallTime())
		assert.Equal(t, uint64(110), stats1.LoadSeries())
		assert.Equal(t, uint64(142), stats1.LoadBytes())
	})

	t.Run("merge two nil stats objects", func(t *testing.T) {
		var stats1 *Stats
		var stats2 *Stats

		stats1.Merge(stats2)

		assert.Equal(t, time.Duration(0), stats1.LoadWallTime())
		assert.Equal(t, uint64(0), stats1.LoadSeries())
		assert.Equal(t, uint64(0), stats1.LoadBytes())
	})
}
