package parquetutil

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/search"
	parquet_storage "github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
)

func TestRowRanges_EncodeDecode(t *testing.T) {
	tests := []struct {
		name      string
		rowRanges []search.RowRange
		encoded   []byte
	}{
		{
			name:      "empty",
			rowRanges: []search.RowRange{},
			encoded: []byte{
				0, 0, 0, 0,
			},
		},
		{
			name: "multiple row ranges",
			rowRanges: []search.RowRange{
				{From: 1, Count: 2},
				{From: 256, Count: 513},
			},
			encoded: []byte{
				2, 0, 0, 0,
				1, 0, 0, 0, 0, 0, 0, 0,
				2, 0, 0, 0, 0, 0, 0, 0,
				0, 1, 0, 0, 0, 0, 0, 0,
				1, 2, 0, 0, 0, 0, 0, 0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := encodeRowRanges(tt.rowRanges)
			require.Equal(t, tt.encoded, encoded)

			decoded, err := decodeRowRanges(encoded)
			require.NoError(t, err)
			require.Equal(t, tt.rowRanges, decoded)
		})
	}
}

func TestRowRanges_InvalidDecode(t *testing.T) {
	tests := []struct {
		name        string
		buf         []byte
		errContains string
	}{
		{
			name:        "short header",
			buf:         []byte{1, 0},
			errContains: "invalid row ranges cache entry: got 2 bytes, expected at least 4",
		},
		{
			name:        "truncated row range",
			buf:         []byte{1, 0, 0, 0},
			errContains: "invalid row ranges cache entry: got 4 bytes for 1 row ranges",
		},
		{
			name:        "trailing bytes",
			buf:         []byte{0, 0, 0, 0, 0},
			errContains: "invalid row ranges cache entry: got 5 bytes, expected 4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decoded, err := decodeRowRanges(tt.buf)
			require.Nil(t, decoded)
			require.ErrorContains(t, err, tt.errContains)
		})
	}
}

func TestRowRangesCacheKey(t *testing.T) {
	cacheName := "parquet-row-ranges"
	shard := fakeParquetShard{name: "block-01/labels.parquet", shardIdx: 7}
	ctx := user.InjectOrgID(context.Background(), "tenant-a")
	constraints := []search.Constraint{
		equalConstraint("labels.foo", "bar baz:qux"),
		equalConstraint("labels.zone", "us-east"),
	}

	key, ok := rowRangesCacheKey(cacheName, ctx, shard, 4, constraints)
	require.True(t, ok)
	require.Regexp(t, `^parquet-row-ranges:[0-9a-f]{64}$`, key)
	require.Len(t, key, len(cacheName)+1+64)
	require.NotContains(t, key, " ")
	require.NotContains(t, key, "bar baz:qux")

	sameKey, ok := rowRangesCacheKey(cacheName, ctx, shard, 4, constraints)
	require.True(t, ok)
	require.Equal(t, key, sameKey)

	differentConstraintKey, ok := rowRangesCacheKey(cacheName, ctx, shard, 4, []search.Constraint{
		equalConstraint("labels.foo", "different"),
		equalConstraint("labels.zone", "us-east"),
	})
	require.True(t, ok)
	require.NotEqual(t, key, differentConstraintKey)

	noConstraintKey, ok := rowRangesCacheKey(cacheName, ctx, shard, 4, nil)
	require.True(t, ok)
	require.Regexp(t, `^parquet-row-ranges:[0-9a-f]{64}$`, noConstraintKey)
	require.NotEqual(t, key, noConstraintKey)
}

func TestRowRangesCacheKey_MissingTenant(t *testing.T) {
	shard := fakeParquetShard{name: "block-01/labels.parquet", shardIdx: 7}

	key, ok := rowRangesCacheKey("parquet-row-ranges", context.Background(), shard, 3, nil)
	require.False(t, ok)
	require.Empty(t, key)
}

func TestRowRangesCache_GetSet(t *testing.T) {
	env := newRowRangesCacheTestEnv(t)
	rowRanges := []search.RowRange{
		{From: 10, Count: 3},
		{From: 30, Count: 4},
	}

	got, ok := env.cache.Get(env.ctx, env.shard, 2, env.constraints)
	require.False(t, ok)
	require.Nil(t, got)

	require.NoError(t, env.cache.Set(env.ctx, env.shard, 2, env.constraints, rowRanges))
	require.Equal(t, time.Hour, env.backingCache.lastTTL)
	require.Len(t, env.backingCache.entries, 1)

	got, ok = env.cache.Get(env.ctx, env.shard, 2, env.constraints)
	require.True(t, ok)
	require.Equal(t, rowRanges, got)

	require.NoError(t, testutil.GatherAndCompare(env.reg, bytes.NewBufferString(`
		# HELP cortex_parquet_row_ranges_cache_hits_total Total number of parquet row ranges cache hits.
		# TYPE cortex_parquet_row_ranges_cache_hits_total counter
		cortex_parquet_row_ranges_cache_hits_total{name="test"} 1
		# HELP cortex_parquet_row_ranges_cache_misses_total Total number of parquet row ranges cache misses.
		# TYPE cortex_parquet_row_ranges_cache_misses_total counter
		cortex_parquet_row_ranges_cache_misses_total{name="test"} 1
	`),
		"cortex_parquet_row_ranges_cache_hits_total",
		"cortex_parquet_row_ranges_cache_misses_total",
	))
}

func TestRowRangesCache_GetCorruptEntry(t *testing.T) {
	env := newRowRangesCacheTestEnv(t)
	key, ok := rowRangesCacheKey("test", env.ctx, env.shard, 2, env.constraints)
	require.True(t, ok)
	env.backingCache.entries[key] = []byte{1, 0, 0, 0}

	got, ok := env.cache.Get(env.ctx, env.shard, 2, env.constraints)
	require.False(t, ok)
	require.Nil(t, got)

	require.NoError(t, testutil.GatherAndCompare(env.reg, bytes.NewBufferString(`
		# HELP cortex_parquet_row_ranges_cache_decode_errors_total Total number of parquet row ranges cache decode errors.
		# TYPE cortex_parquet_row_ranges_cache_decode_errors_total counter
		cortex_parquet_row_ranges_cache_decode_errors_total{name="test"} 1
		# HELP cortex_parquet_row_ranges_cache_misses_total Total number of parquet row ranges cache misses.
		# TYPE cortex_parquet_row_ranges_cache_misses_total counter
		cortex_parquet_row_ranges_cache_misses_total{name="test"} 1
	`),
		"cortex_parquet_row_ranges_cache_decode_errors_total",
		"cortex_parquet_row_ranges_cache_misses_total",
	))
}

func TestRowRangesCache_MissingTenant(t *testing.T) {
	env := newRowRangesCacheTestEnv(t)
	rowRanges := []search.RowRange{{From: 10, Count: 3}}

	require.NoError(t, env.cache.Set(context.Background(), env.shard, 2, env.constraints, rowRanges))
	require.Empty(t, env.backingCache.entries)

	got, ok := env.cache.Get(context.Background(), env.shard, 2, env.constraints)
	require.False(t, ok)
	require.Nil(t, got)
	require.Equal(t, 0, env.backingCache.fetchCalls)

	require.NoError(t, testutil.GatherAndCompare(env.reg, bytes.NewBufferString(`
		# HELP cortex_parquet_row_ranges_cache_misses_total Total number of parquet row ranges cache misses.
		# TYPE cortex_parquet_row_ranges_cache_misses_total counter
		cortex_parquet_row_ranges_cache_misses_total{name="test"} 1
	`),
		"cortex_parquet_row_ranges_cache_misses_total",
	))
}

func TestNewRowRangesCache_NilBackend(t *testing.T) {
	require.Nil(t, NewRowRangesCache(nil, "test", time.Hour, prometheus.NewRegistry()))
}

func equalConstraint(path, value string) search.Constraint {
	return search.Equal(path, parquet.ValueOf(value))
}

type rowRangesCacheTestEnv struct {
	reg          *prometheus.Registry
	backingCache *fakeThanosCache
	cache        *RowRangesCache
	ctx          context.Context
	shard        fakeParquetShard
	constraints  []search.Constraint
}

func newRowRangesCacheTestEnv(t *testing.T) rowRangesCacheTestEnv {
	t.Helper()

	reg := prometheus.NewRegistry()
	backingCache := newFakeThanosCache()

	return rowRangesCacheTestEnv{
		reg:          reg,
		backingCache: backingCache,
		cache:        NewRowRangesCache(backingCache, "test", time.Hour, reg),
		ctx:          user.InjectOrgID(context.Background(), "tenant-a"),
		shard:        fakeParquetShard{name: "block-01/labels.parquet", shardIdx: 7},
		constraints:  []search.Constraint{equalConstraint("labels.foo", "bar")},
	}
}

type fakeThanosCache struct {
	entries    map[string][]byte
	lastTTL    time.Duration
	fetchCalls int
}

func newFakeThanosCache() *fakeThanosCache {
	return &fakeThanosCache{
		entries: map[string][]byte{},
	}
}

func (c *fakeThanosCache) Store(data map[string][]byte, ttl time.Duration) {
	c.lastTTL = ttl
	for key, value := range data {
		c.entries[key] = value
	}
}

func (c *fakeThanosCache) Fetch(_ context.Context, keys []string) map[string][]byte {
	c.fetchCalls++

	hits := map[string][]byte{}
	for _, key := range keys {
		value, ok := c.entries[key]
		if ok {
			hits[key] = value
		}
	}
	return hits
}

func (c *fakeThanosCache) Name() string {
	return "fake"
}

type fakeParquetShard struct {
	name     string
	shardIdx int
}

func (s fakeParquetShard) Name() string {
	return s.name
}

func (s fakeParquetShard) ShardIdx() int {
	return s.shardIdx
}

func (s fakeParquetShard) LabelsFile() parquet_storage.ParquetFileView {
	return nil
}

func (s fakeParquetShard) ChunksFile() parquet_storage.ParquetFileView {
	return nil
}

func (s fakeParquetShard) TSDBSchema() (*schema.TSDBSchema, error) {
	return nil, nil
}
