package parquetutil

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"strconv"
	"time"

	"github.com/prometheus-community/parquet-common/search"
	parquet_storage "github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/cache"

	"github.com/cortexproject/cortex/pkg/util/users"
)

const (
	// 4 bytes uint32 count.
	rowRangesHeaderSize = 4

	// int64 From + int64 Count.
	rowRangeEntrySize = 16
)

type rowRangesCacheMetrics struct {
	hits         *prometheus.CounterVec
	misses       *prometheus.CounterVec
	decodeErrors *prometheus.CounterVec
}

func newRowRangesCacheMetrics(r prometheus.Registerer) *rowRangesCacheMetrics {
	return &rowRangesCacheMetrics{
		hits: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_parquet_row_ranges_cache_hits_total",
			Help: "Total number of parquet row ranges cache hits.",
		}, []string{"name"}),
		misses: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_parquet_row_ranges_cache_misses_total",
			Help: "Total number of parquet row ranges cache misses.",
		}, []string{"name"}),
		decodeErrors: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_parquet_row_ranges_cache_decode_errors_total",
			Help: "Total number of parquet row ranges cache decode errors.",
		}, []string{"name"}),
	}
}

type RowRangesCache struct {
	cache   cache.Cache
	name    string
	metrics *rowRangesCacheMetrics
	ttl     time.Duration
}

func NewRowRangesCache(cache cache.Cache, name string, ttl time.Duration, reg prometheus.Registerer) *RowRangesCache {
	if cache == nil {
		return nil
	}

	return &RowRangesCache{
		cache:   cache,
		name:    name,
		metrics: newRowRangesCacheMetrics(reg),
		ttl:     ttl,
	}
}

func (c *RowRangesCache) Get(ctx context.Context, shard parquet_storage.ParquetShard, rgIdx int, cs []search.Constraint) ([]search.RowRange, bool) {
	key, ok := rowRangesCacheKey(c.name, ctx, shard, rgIdx, cs)
	if !ok {
		c.metrics.misses.WithLabelValues(c.name).Inc()
		return nil, false
	}

	hits := c.cache.Fetch(ctx, []string{key})
	buf, ok := hits[key]
	if !ok {
		c.metrics.misses.WithLabelValues(c.name).Inc()
		return nil, false
	}

	rowRanges, err := decodeRowRanges(buf)
	if err != nil {
		c.metrics.decodeErrors.WithLabelValues(c.name).Inc()
		c.metrics.misses.WithLabelValues(c.name).Inc()
		return nil, false
	}

	c.metrics.hits.WithLabelValues(c.name).Inc()
	return rowRanges, true
}

func (c *RowRangesCache) Set(ctx context.Context, shard parquet_storage.ParquetShard, rgIdx int, cs []search.Constraint, rr []search.RowRange) error {
	key, ok := rowRangesCacheKey(c.name, ctx, shard, rgIdx, cs)
	if !ok {
		return nil
	}

	c.cache.Store(map[string][]byte{key: encodeRowRanges(rr)}, c.ttl)
	return nil
}

func (c *RowRangesCache) Delete(context.Context, parquet_storage.ParquetShard, int, []search.Constraint) error {
	// thanos/pkg/cache.Cache doesn't expose deletion.
	return nil
}

func (c *RowRangesCache) Close() error {
	// thanos/pkg/cache.Cache doesn't expose Close.
	return nil
}

func rowRangesCacheKey(name string, ctx context.Context, shard parquet_storage.ParquetShard, rgIdx int, cs []search.Constraint) (string, bool) {
	userID, err := users.TenantID(ctx)
	if err != nil {
		return "", false
	}

	//gomemcache rejects whitespace or keys over 250 bytes, so hash the key
	h := sha256.New()
	writeCacheKeyPart(h, userID)
	writeCacheKeyPart(h, shard.Name())
	writeCacheKeyPart(h, strconv.Itoa(shard.ShardIdx()))
	writeCacheKeyPart(h, strconv.Itoa(rgIdx))
	for _, c := range cs {
		writeCacheKeyPart(h, c.String())
	}

	return name + ":" + hex.EncodeToString(h.Sum(nil)), true
}

func writeCacheKeyPart(h hash.Hash, part string) {
	_, _ = fmt.Fprintf(h, "%d:", len(part))
	_, _ = h.Write([]byte(part))
}

func encodeRowRanges(rowRanges []search.RowRange) []byte {
	buf := make([]byte, rowRangesHeaderSize+len(rowRanges)*rowRangeEntrySize)
	binary.LittleEndian.PutUint32(buf[:rowRangesHeaderSize], uint32(len(rowRanges)))

	offset := rowRangesHeaderSize
	for _, rowRange := range rowRanges {
		binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(rowRange.From))
		binary.LittleEndian.PutUint64(buf[offset+8:offset+rowRangeEntrySize], uint64(rowRange.Count))
		offset += rowRangeEntrySize
	}

	return buf
}

func decodeRowRanges(buf []byte) ([]search.RowRange, error) {
	if len(buf) < rowRangesHeaderSize {
		return nil, fmt.Errorf("invalid row ranges cache entry: got %d bytes, expected at least %d", len(buf), rowRangesHeaderSize)
	}

	count := binary.LittleEndian.Uint32(buf[:rowRangesHeaderSize])
	if count > uint32((len(buf)-rowRangesHeaderSize)/rowRangeEntrySize) {
		return nil, fmt.Errorf("invalid row ranges cache entry: got %d bytes for %d row ranges", len(buf), count)
	}

	expectedSize := rowRangesHeaderSize + int(count)*rowRangeEntrySize
	if len(buf) != expectedSize {
		return nil, fmt.Errorf("invalid row ranges cache entry: got %d bytes, expected %d", len(buf), expectedSize)
	}

	rowRanges := make([]search.RowRange, int(count))
	offset := rowRangesHeaderSize
	for i := range rowRanges {
		rowRanges[i] = search.RowRange{
			From:  int64(binary.LittleEndian.Uint64(buf[offset : offset+8])),
			Count: int64(binary.LittleEndian.Uint64(buf[offset+8 : offset+rowRangeEntrySize])),
		}
		offset += rowRangeEntrySize
	}

	return rowRanges, nil
}
