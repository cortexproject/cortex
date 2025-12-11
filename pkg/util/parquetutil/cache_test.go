package parquetutil

import (
	"bytes"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func Test_Cache_LRUEviction(t *testing.T) {
	reg := prometheus.NewRegistry()
	cfg := &CacheConfig{
		ParquetShardCacheSize: 2,
		ParquetShardCacheTTL:  0,
		MaintenanceInterval:   time.Minute,
	}
	cache, err := NewCache[string](cfg, "test", reg)
	require.NoError(t, err)
	defer cache.Close()

	cache.Set("key1", "value1")
	cache.Set("key2", "value2")

	_ = cache.Get("key1") // hit
	// "key2" deleted by LRU eviction
	cache.Set("key3", "value3")

	val1 := cache.Get("key1") // hit
	require.Equal(t, "value1", val1)
	val3 := cache.Get("key3") // hit
	require.Equal(t, "value3", val3)
	val2 := cache.Get("key2") // miss
	require.Equal(t, "", val2)

	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_parquet_cache_evictions_total Total number of parquet cache evictions
		# TYPE cortex_parquet_cache_evictions_total counter
		cortex_parquet_cache_evictions_total{name="test"} 1
		# HELP cortex_parquet_cache_hits_total Total number of parquet cache hits
		# TYPE cortex_parquet_cache_hits_total counter
		cortex_parquet_cache_hits_total{name="test"} 3
		# HELP cortex_parquet_cache_item_count Current number of cached parquet items
		# TYPE cortex_parquet_cache_item_count gauge
		cortex_parquet_cache_item_count{name="test"} 2
		# HELP cortex_parquet_cache_misses_total Total number of parquet cache misses
		# TYPE cortex_parquet_cache_misses_total counter
		cortex_parquet_cache_misses_total{name="test"} 1
	`)))
}

func Test_Cache_TTLEvictionByGet(t *testing.T) {
	reg := prometheus.NewRegistry()
	cfg := &CacheConfig{
		ParquetShardCacheSize: 10,
		ParquetShardCacheTTL:  100 * time.Millisecond,
		MaintenanceInterval:   time.Minute,
	}

	cache, err := NewCache[string](cfg, "test", reg)
	require.NoError(t, err)
	defer cache.Close()

	cache.Set("key1", "value1")

	val := cache.Get("key1")
	require.Equal(t, "value1", val)

	// sleep longer than TTL
	time.Sleep(150 * time.Millisecond)

	val = cache.Get("key1")
	require.Equal(t, "", val)

	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_parquet_cache_evictions_total Total number of parquet cache evictions
		# TYPE cortex_parquet_cache_evictions_total counter
		cortex_parquet_cache_evictions_total{name="test"} 1
		# HELP cortex_parquet_cache_hits_total Total number of parquet cache hits
		# TYPE cortex_parquet_cache_hits_total counter
		cortex_parquet_cache_hits_total{name="test"} 1
		# HELP cortex_parquet_cache_item_count Current number of cached parquet items
		# TYPE cortex_parquet_cache_item_count gauge
		cortex_parquet_cache_item_count{name="test"} 0
		# HELP cortex_parquet_cache_misses_total Total number of parquet cache misses
		# TYPE cortex_parquet_cache_misses_total counter
		cortex_parquet_cache_misses_total{name="test"} 1
	`)))
}

func Test_Cache_TTLEvictionByLoop(t *testing.T) {
	reg := prometheus.NewRegistry()
	cfg := &CacheConfig{
		ParquetShardCacheSize: 10,
		ParquetShardCacheTTL:  100 * time.Millisecond,
		MaintenanceInterval:   100 * time.Millisecond,
	}

	cache, err := NewCache[string](cfg, "test", reg)
	require.NoError(t, err)
	defer cache.Close()

	cache.Set("key1", "value1")

	val := cache.Get("key1")
	require.Equal(t, "value1", val)

	// sleep longer than TTL
	time.Sleep(150 * time.Millisecond)

	if c, ok := cache.(*Cache[string]); ok {
		// should delete by maintenance loop
		_, ok := c.cache.Peek("key1")
		require.False(t, ok)
	}

	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_parquet_cache_evictions_total Total number of parquet cache evictions
		# TYPE cortex_parquet_cache_evictions_total counter
		cortex_parquet_cache_evictions_total{name="test"} 1
		# HELP cortex_parquet_cache_hits_total Total number of parquet cache hits
		# TYPE cortex_parquet_cache_hits_total counter
		cortex_parquet_cache_hits_total{name="test"} 1
		# HELP cortex_parquet_cache_item_count Current number of cached parquet items
		# TYPE cortex_parquet_cache_item_count gauge
		cortex_parquet_cache_item_count{name="test"} 0
	`)))
}
