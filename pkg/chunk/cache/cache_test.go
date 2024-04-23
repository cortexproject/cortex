package cache_test

import (
	"context"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/chunk/cache"
)

func fillCache(t *testing.T, cache cache.Cache) ([]string, []chunkenc.Chunk) {
	const chunkLen = 13 * 3600 // in seconds

	// put a set of chunks, larger than background batch size, with varying timestamps and values
	keys := []string{}
	bufs := [][]byte{}
	chunks := []chunkenc.Chunk{}
	for i := 0; i < 111; i++ {
		ts := model.TimeFromUnix(int64(i * chunkLen))
		promChunk := chunkenc.NewXORChunk()
		appender, err := promChunk.Appender()
		require.NoError(t, err)
		appender.Append(int64(ts), float64(i))

		cleanChunk, err := chunkenc.FromData(promChunk.Encoding(), promChunk.Bytes())
		require.NoError(t, err)
		keys = append(keys, strconv.Itoa(i))
		bufs = append(bufs, promChunk.Bytes())
		chunks = append(chunks, cleanChunk)
	}

	cache.Store(context.Background(), keys, bufs)
	return keys, chunks
}

func testCacheSingle(t *testing.T, cache cache.Cache, keys []string, chunks []chunkenc.Chunk) {
	for i := 0; i < 100; i++ {
		index := rand.Intn(len(keys))
		key := keys[index]

		found, bufs, missingKeys := cache.Fetch(context.Background(), []string{key})
		require.Len(t, found, 1)
		require.Len(t, bufs, 1)
		require.Len(t, missingKeys, 0)

		c, err := chunkenc.FromData(chunkenc.EncXOR, bufs[0])
		require.NoError(t, err)
		require.Equal(t, chunks[index], c)
	}
}

func testCacheMultiple(t *testing.T, cache cache.Cache, keys []string, chunks []chunkenc.Chunk) {
	// test getting them all
	found, bufs, missingKeys := cache.Fetch(context.Background(), keys)
	require.Len(t, found, len(keys))
	require.Len(t, bufs, len(keys))
	require.Len(t, missingKeys, 0)

	result := []chunkenc.Chunk{}
	for i := range found {
		c, err := chunkenc.FromData(chunkenc.EncXOR, bufs[i])
		require.NoError(t, err)
		result = append(result, c)
	}
	require.Equal(t, chunks, result)
}

func testCacheMiss(t *testing.T, cache cache.Cache) {
	for i := 0; i < 100; i++ {
		key := strconv.Itoa(rand.Int()) // arbitrary key which should fail: no chunk key is a single integer
		found, bufs, missing := cache.Fetch(context.Background(), []string{key})
		require.Empty(t, found)
		require.Empty(t, bufs)
		require.Len(t, missing, 1)
	}
}

func testCache(t *testing.T, cache cache.Cache) {
	keys, chunks := fillCache(t, cache)
	t.Run("Single", func(t *testing.T) {
		testCacheSingle(t, cache, keys, chunks)
	})
	t.Run("Multiple", func(t *testing.T) {
		testCacheMultiple(t, cache, keys, chunks)
	})
	t.Run("Miss", func(t *testing.T) {
		testCacheMiss(t, cache)
	})
}

func TestMemcache(t *testing.T) {
	t.Run("Unbatched", func(t *testing.T) {
		cache := cache.NewMemcached(cache.MemcachedConfig{}, newMockMemcache(),
			"test", nil, log.NewNopLogger())
		testCache(t, cache)
	})

	t.Run("Batched", func(t *testing.T) {
		cache := cache.NewMemcached(cache.MemcachedConfig{
			BatchSize:   10,
			Parallelism: 3,
		}, newMockMemcache(), "test", nil, log.NewNopLogger())
		testCache(t, cache)
	})
}

func TestFifoCache(t *testing.T) {
	cache := cache.NewFifoCache("test", cache.FifoCacheConfig{MaxSizeItems: 1e3, Validity: 1 * time.Hour},
		nil, log.NewNopLogger())
	testCache(t, cache)
}

func TestSnappyCache(t *testing.T) {
	cache := cache.NewSnappy(cache.NewMockCache(), log.NewNopLogger())
	testCache(t, cache)
}
