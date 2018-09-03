package cache_test

import (
	"context"
	"math/rand"
	"os"
	"path"
	"sort"
	"strconv"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/chunk/cache"
	prom_chunk "github.com/weaveworks/cortex/pkg/prom1/storage/local/chunk"
)

const userID = "1"

func fillCache(t *testing.T, cache cache.Cache) ([]string, []chunk.Chunk) {
	const chunkLen = 13 * 3600 // in seconds

	// put 100 chunks from 0 to 99
	keys := []string{}
	chunks := []chunk.Chunk{}
	for i := 0; i < 100; i++ {
		ts := model.TimeFromUnix(int64(i * chunkLen))
		promChunk, _ := prom_chunk.New().Add(model.SamplePair{
			Timestamp: ts,
			Value:     model.SampleValue(i),
		})
		c := chunk.NewChunk(
			userID,
			model.Fingerprint(1),
			model.Metric{
				model.MetricNameLabel: "foo",
				"bar": "baz",
			},
			promChunk[0],
			ts,
			ts.Add(chunkLen),
		)

		buf, err := c.Encode()
		require.NoError(t, err)

		key := c.ExternalKey()
		err = cache.Store(context.Background(), key, buf)
		require.NoError(t, err)

		keys = append(keys, key)
		chunks = append(chunks, c)
	}

	return keys, chunks
}

func testCacheSingle(t *testing.T, cache cache.Cache, keys []string, chunks []chunk.Chunk) {
	for i := 0; i < 100; i++ {
		index := rand.Intn(len(keys))
		key := keys[index]

		found, bufs, missingKeys, err := cache.Fetch(context.Background(), []string{key})
		require.NoError(t, err)
		require.Len(t, found, 1)
		require.Len(t, bufs, 1)
		require.Len(t, missingKeys, 0)

		c, err := chunk.ParseExternalKey(userID, found[0])
		require.NoError(t, err)
		err = c.Decode(chunk.NewDecodeContext(), bufs[0])
		require.NoError(t, err)
		require.Equal(t, c, chunks[index])
	}
}

func testCacheMultiple(t *testing.T, cache cache.Cache, keys []string, chunks []chunk.Chunk) {
	// test getting them all
	found, bufs, missingKeys, err := cache.Fetch(context.Background(), keys)
	require.NoError(t, err)
	require.Len(t, found, len(keys))
	require.Len(t, bufs, len(keys))
	require.Len(t, missingKeys, 0)

	result := []chunk.Chunk{}
	for i := range found {
		c, err := chunk.ParseExternalKey(userID, found[i])
		require.NoError(t, err)
		err = c.Decode(chunk.NewDecodeContext(), bufs[i])
		require.NoError(t, err)
		result = append(result, c)
	}
	require.Equal(t, chunks, result)
}

func testChunkFetcher(t *testing.T, c cache.Cache, keys []string, chunks []chunk.Chunk) {
	fetcher, err := chunk.NewChunkFetcher(cache.Config{
		Cache: c,
	}, nil)
	require.NoError(t, err)
	defer fetcher.Stop()

	found, err := fetcher.FetchChunks(context.Background(), chunks, keys)
	require.NoError(t, err)
	sort.Sort(byExternalKey(found))
	sort.Sort(byExternalKey(chunks))
	require.Equal(t, chunks, found)
}

type byExternalKey []chunk.Chunk

func (a byExternalKey) Len() int           { return len(a) }
func (a byExternalKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byExternalKey) Less(i, j int) bool { return a[i].ExternalKey() < a[j].ExternalKey() }

func testCacheMiss(t *testing.T, cache cache.Cache) {
	for i := 0; i < 100; i++ {
		key := strconv.Itoa(rand.Int())
		found, bufs, missing, err := cache.Fetch(context.Background(), []string{key})
		require.NoError(t, err)
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
	t.Run("Fetcher", func(t *testing.T) {
		testChunkFetcher(t, cache, keys, chunks)
	})
}

func TestMemcache(t *testing.T) {
	cache := cache.NewMemcached(cache.MemcachedConfig{}, newMockMemcache())
	testCache(t, cache)
}

func TestDiskcache(t *testing.T) {
	dirname := os.TempDir()
	filename := path.Join(dirname, "diskcache")
	defer os.RemoveAll(filename)

	cache, err := cache.NewDiskcache(cache.DiskcacheConfig{
		Path: filename,
		Size: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	testCache(t, cache)
}
