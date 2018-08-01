package querier

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaveworks/cortex/pkg/chunk"
	promchunk "github.com/weaveworks/cortex/pkg/prom1/storage/local/chunk"
)

func TestChunkMergeIterator(t *testing.T) {
	for i, tc := range []struct {
		chunks     []chunk.Chunk
		mint, maxt int64
	}{
		{
			chunks: []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, promchunk.Varbit),
			},
			maxt: 100,
		},

		{
			chunks: []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, promchunk.Varbit),
				mkChunk(t, 0, 100, 1*time.Millisecond, promchunk.Varbit),
			},
			maxt: 100,
		},

		{
			chunks: []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, promchunk.Varbit),
				mkChunk(t, 50, 150, 1*time.Millisecond, promchunk.Varbit),
				mkChunk(t, 100, 200, 1*time.Millisecond, promchunk.Varbit),
			},
			maxt: 200,
		},

		{
			chunks: []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, promchunk.Varbit),
				mkChunk(t, 100, 200, 1*time.Millisecond, promchunk.Varbit),
			},
			maxt: 200,
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			iter := newChunkMergeIteratorV2(tc.chunks)
			for i := tc.mint; i < tc.maxt; i++ {
				require.True(t, iter.Next())
				ts, s := iter.At()
				assert.Equal(t, i, ts)
				assert.Equal(t, float64(i), s)
				assert.NoError(t, iter.Err())
			}
			assert.False(t, iter.Next())
		})
	}
}

func TestChunkMergeIteratorSeek(t *testing.T) {
	iter := newChunkMergeIteratorV2([]chunk.Chunk{
		mkChunk(t, 0, 100, 1*time.Millisecond, promchunk.Varbit),
		mkChunk(t, 50, 150, 1*time.Millisecond, promchunk.Varbit),
		mkChunk(t, 100, 200, 1*time.Millisecond, promchunk.Varbit),
	})

	for i := int64(0); i < 10; i += 20 {
		require.True(t, iter.Seek(i))
		ts, s := iter.At()
		assert.Equal(t, i, ts)
		assert.Equal(t, float64(i), s)
		assert.NoError(t, iter.Err())

		for j := i + 1; j < 200; j++ {
			require.True(t, iter.Next())
			ts, s := iter.At()
			assert.Equal(t, j, ts)
			assert.Equal(t, float64(j), s)
			assert.NoError(t, iter.Err())
		}
		assert.False(t, iter.Next())
	}
}
