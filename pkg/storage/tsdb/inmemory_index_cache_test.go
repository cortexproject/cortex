package tsdb

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/tenancy"
)

func TestInMemoryIndexCache_UpdateItem(t *testing.T) {
	var errorLogs []string
	errorLogger := log.LoggerFunc(func(kvs ...interface{}) error {
		var lvl string
		for i := 0; i < len(kvs); i += 2 {
			if kvs[i] == "level" {
				lvl = fmt.Sprint(kvs[i+1])
				break
			}
		}
		if lvl != "error" {
			return nil
		}
		var buf bytes.Buffer
		defer func() { errorLogs = append(errorLogs, buf.String()) }()
		return log.NewLogfmtLogger(&buf).Log(kvs...)
	})

	metrics := prometheus.NewRegistry()
	cache, err := NewInMemoryIndexCacheWithConfig(log.NewSyncLogger(errorLogger), nil, metrics, storecache.InMemoryIndexCacheConfig{
		MaxItemSize: 1024,
		MaxSize:     1024,
	})
	testutil.Ok(t, err)

	uid := func(id storage.SeriesRef) ulid.ULID { return ulid.MustNew(uint64(id), nil) }
	lbl := labels.Label{Name: "foo", Value: "bar"}
	matcher := labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")
	ctx := context.Background()

	for _, tt := range []struct {
		typ string
		set func(storage.SeriesRef, []byte)
		get func(storage.SeriesRef) ([]byte, bool)
	}{
		{
			typ: storecache.CacheTypePostings,
			set: func(id storage.SeriesRef, b []byte) { cache.StorePostings(uid(id), lbl, b, tenancy.DefaultTenant) },
			get: func(id storage.SeriesRef) ([]byte, bool) {
				hits, _ := cache.FetchMultiPostings(ctx, uid(id), []labels.Label{lbl}, tenancy.DefaultTenant)
				b, ok := hits[lbl]

				return b, ok
			},
		},
		{
			typ: storecache.CacheTypeSeries,
			set: func(id storage.SeriesRef, b []byte) { cache.StoreSeries(uid(id), id, b, tenancy.DefaultTenant) },
			get: func(id storage.SeriesRef) ([]byte, bool) {
				hits, _ := cache.FetchMultiSeries(ctx, uid(id), []storage.SeriesRef{id}, tenancy.DefaultTenant)
				b, ok := hits[id]

				return b, ok
			},
		},
		{
			typ: storecache.CacheTypeExpandedPostings,
			set: func(id storage.SeriesRef, b []byte) {
				cache.StoreExpandedPostings(uid(id), []*labels.Matcher{matcher}, b, tenancy.DefaultTenant)
			},
			get: func(id storage.SeriesRef) ([]byte, bool) {
				return cache.FetchExpandedPostings(ctx, uid(id), []*labels.Matcher{matcher}, tenancy.DefaultTenant)
			},
		},
	} {
		t.Run(tt.typ, func(t *testing.T) {
			defer func() { errorLogs = nil }()

			// Set value.
			tt.set(0, []byte{0})
			buf, ok := tt.get(0)
			testutil.Equals(t, true, ok)
			testutil.Equals(t, []byte{0}, buf)
			testutil.Equals(t, []string(nil), errorLogs)

			// Set the same value again.
			tt.set(0, []byte{0})
			buf, ok = tt.get(0)
			testutil.Equals(t, true, ok)
			testutil.Equals(t, []byte{0}, buf)
			testutil.Equals(t, []string(nil), errorLogs)

			// Set a larger value.
			tt.set(1, []byte{0, 1})
			buf, ok = tt.get(1)
			testutil.Equals(t, true, ok)
			testutil.Equals(t, []byte{0, 1}, buf)
			testutil.Equals(t, []string(nil), errorLogs)

			// Mutations to existing values will be ignored.
			tt.set(1, []byte{1, 2})
			buf, ok = tt.get(1)
			testutil.Equals(t, true, ok)
			testutil.Equals(t, []byte{0, 1}, buf)
			testutil.Equals(t, []string(nil), errorLogs)
		})
	}
}

func TestInMemoryIndexCacheSetOverflow(t *testing.T) {
	config := storecache.InMemoryIndexCacheConfig{
		MaxSize:     storecache.DefaultInMemoryIndexCacheConfig.MaxSize,
		MaxItemSize: 100,
	}
	cache, err := NewInMemoryIndexCacheWithConfig(log.NewNopLogger(), nil, nil, config)
	testutil.Ok(t, err)
	counter := cache.overflow.WithLabelValues(storecache.CacheTypeSeries)
	id := ulid.MustNew(ulid.Now(), nil)
	// Insert a small value won't trigger item overflow.
	cache.StoreSeries(id, 1, []byte("0"), tenancy.DefaultTenant)
	testutil.Equals(t, float64(0), prom_testutil.ToFloat64(counter))

	var sb strings.Builder
	for i := 0; i < 100; i++ {
		sb.WriteString(strconv.Itoa(i))
	}
	// Trigger overflow with a large value.
	cache.StoreSeries(id, 2, []byte(sb.String()), tenancy.DefaultTenant)
	testutil.Equals(t, float64(1), prom_testutil.ToFloat64(counter))
}

func BenchmarkInMemoryIndexCacheStore(b *testing.B) {
	logger := log.NewNopLogger()
	cfg := InMemoryIndexCacheConfig{
		MaxSizeBytes: uint64(storecache.DefaultInMemoryIndexCacheConfig.MaxSize),
	}

	blockID := ulid.MustNew(ulid.Now(), nil)
	r := rand.New(rand.NewSource(time.Now().Unix()))
	// 1KB is a common size for series
	seriesData := make([]byte, 1024)
	r.Read(seriesData)
	// 10MB might happen for large postings.
	postingData := make([]byte, 10*1024*1024)
	r.Read(postingData)

	b.Run("FastCache", func(b *testing.B) {
		cache, err := newInMemoryIndexCache(cfg, logger, prometheus.NewRegistry())
		require.NoError(b, err)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cache.StoreSeries(blockID, storage.SeriesRef(i), seriesData, tenancy.DefaultTenant)
		}
	})

	b.Run("ThanosCache", func(b *testing.B) {
		cache, err := storecache.NewInMemoryIndexCacheWithConfig(logger, nil, prometheus.NewRegistry(), storecache.DefaultInMemoryIndexCacheConfig)
		require.NoError(b, err)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cache.StoreSeries(blockID, storage.SeriesRef(i), seriesData, tenancy.DefaultTenant)
		}
	})

	b.Run("FastCacheLargeItem", func(b *testing.B) {
		cache, err := newInMemoryIndexCache(cfg, logger, prometheus.NewRegistry())
		require.NoError(b, err)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cache.StoreSeries(blockID, storage.SeriesRef(i), postingData, tenancy.DefaultTenant)
		}
	})

	b.Run("ThanosCacheLargeItem", func(b *testing.B) {
		cache, err := storecache.NewInMemoryIndexCacheWithConfig(logger, nil, prometheus.NewRegistry(), storecache.DefaultInMemoryIndexCacheConfig)
		require.NoError(b, err)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cache.StoreSeries(blockID, storage.SeriesRef(i), postingData, tenancy.DefaultTenant)
		}
	})
}

func BenchmarkInMemoryIndexCacheStoreConcurrent(b *testing.B) {
	logger := log.NewNopLogger()
	cfg := InMemoryIndexCacheConfig{
		MaxSizeBytes: uint64(storecache.DefaultInMemoryIndexCacheConfig.MaxSize),
	}

	blockID := ulid.MustNew(ulid.Now(), nil)
	r := rand.New(rand.NewSource(time.Now().Unix()))
	// 1KB is a common size for series
	seriesData := make([]byte, 1024)
	r.Read(seriesData)
	// 10MB might happen for large postings.
	postingData := make([]byte, 10*1024*1024)
	r.Read(postingData)

	b.Run("FastCache", func(b *testing.B) {
		cache, err := newInMemoryIndexCache(cfg, logger, prometheus.NewRegistry())
		require.NoError(b, err)
		ch := make(chan int)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < 500; i++ {
			go func() {
				for j := range ch {
					cache.StoreSeries(blockID, storage.SeriesRef(j), seriesData, tenancy.DefaultTenant)
					testutil.Ok(b, err)
				}
			}()
		}

		for i := 0; i < b.N; i++ {
			ch <- i
		}
		close(ch)
	})

	b.Run("ThanosCache", func(b *testing.B) {
		cache, err := storecache.NewInMemoryIndexCacheWithConfig(logger, nil, prometheus.NewRegistry(), storecache.DefaultInMemoryIndexCacheConfig)
		require.NoError(b, err)
		ch := make(chan int)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < 500; i++ {
			go func() {
				for j := range ch {
					cache.StoreSeries(blockID, storage.SeriesRef(j), seriesData, tenancy.DefaultTenant)
					testutil.Ok(b, err)
				}
			}()
		}

		for i := 0; i < b.N; i++ {
			ch <- i
		}
		close(ch)
	})

	b.Run("FastCacheLargeItem", func(b *testing.B) {
		cache, err := newInMemoryIndexCache(cfg, logger, prometheus.NewRegistry())
		require.NoError(b, err)
		ch := make(chan int)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < 500; i++ {
			go func() {
				for j := range ch {
					cache.StoreSeries(blockID, storage.SeriesRef(j), postingData, tenancy.DefaultTenant)
					testutil.Ok(b, err)
				}
			}()
		}

		for i := 0; i < b.N; i++ {
			ch <- i
		}
		close(ch)
	})

	b.Run("ThanosCacheLargeItem", func(b *testing.B) {
		cache, err := storecache.NewInMemoryIndexCacheWithConfig(logger, nil, prometheus.NewRegistry(), storecache.DefaultInMemoryIndexCacheConfig)
		require.NoError(b, err)
		ch := make(chan int)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < 500; i++ {
			go func() {
				for j := range ch {
					cache.StoreSeries(blockID, storage.SeriesRef(j), postingData, tenancy.DefaultTenant)
					testutil.Ok(b, err)
				}
			}()
		}

		for i := 0; i < b.N; i++ {
			ch <- i
		}
		close(ch)
	})
}

func BenchmarkInMemoryIndexCacheFetch(b *testing.B) {
	logger := log.NewNopLogger()
	cfg := InMemoryIndexCacheConfig{
		MaxSizeBytes: uint64(storecache.DefaultInMemoryIndexCacheConfig.MaxSize),
	}

	blockID := ulid.MustNew(ulid.Now(), nil)
	r := rand.New(rand.NewSource(time.Now().Unix()))
	// 1KB is a common size for series
	seriesData := make([]byte, 1024)
	r.Read(seriesData)
	ctx := context.Background()
	items := 10000
	ids := make([]storage.SeriesRef, items)
	for i := 0; i < items; i++ {
		ids[i] = storage.SeriesRef(i)
	}

	b.Run("FastCache", func(b *testing.B) {
		cache, err := newInMemoryIndexCache(cfg, logger, prometheus.NewRegistry())
		require.NoError(b, err)
		for i := 0; i < items; i++ {
			cache.StoreSeries(blockID, storage.SeriesRef(i), seriesData, tenancy.DefaultTenant)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cache.FetchMultiSeries(ctx, blockID, ids, tenancy.DefaultTenant)
		}
	})

	b.Run("ThanosCache", func(b *testing.B) {
		cache, err := storecache.NewInMemoryIndexCacheWithConfig(logger, nil, prometheus.NewRegistry(), storecache.DefaultInMemoryIndexCacheConfig)
		require.NoError(b, err)
		for i := 0; i < items; i++ {
			cache.StoreSeries(blockID, storage.SeriesRef(i), seriesData, tenancy.DefaultTenant)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cache.FetchMultiSeries(ctx, blockID, ids, tenancy.DefaultTenant)
		}
	})
}

func BenchmarkInMemoryIndexCacheFetchConcurrent(b *testing.B) {
	logger := log.NewNopLogger()
	cfg := InMemoryIndexCacheConfig{
		MaxSizeBytes: uint64(storecache.DefaultInMemoryIndexCacheConfig.MaxSize),
	}

	blockID := ulid.MustNew(ulid.Now(), nil)
	r := rand.New(rand.NewSource(time.Now().Unix()))
	// 1KB is a common size for series
	seriesData := make([]byte, 1024)
	r.Read(seriesData)
	ctx := context.Background()
	items := 10000
	ids := make([]storage.SeriesRef, items)
	for i := 0; i < items; i++ {
		ids[i] = storage.SeriesRef(i)
	}

	b.Run("FastCache", func(b *testing.B) {
		cache, err := newInMemoryIndexCache(cfg, logger, prometheus.NewRegistry())
		require.NoError(b, err)
		for i := 0; i < items; i++ {
			cache.StoreSeries(blockID, storage.SeriesRef(i), seriesData, tenancy.DefaultTenant)
		}
		b.ReportAllocs()
		b.ResetTimer()

		ch := make(chan int)
		for i := 0; i < 500; i++ {
			go func() {
				for range ch {
					cache.FetchMultiSeries(ctx, blockID, ids, tenancy.DefaultTenant)
				}
			}()
		}

		for i := 0; i < b.N; i++ {
			ch <- i
		}
		close(ch)
	})

	b.Run("ThanosCache", func(b *testing.B) {
		cache, err := storecache.NewInMemoryIndexCacheWithConfig(logger, nil, prometheus.NewRegistry(), storecache.DefaultInMemoryIndexCacheConfig)
		require.NoError(b, err)
		for i := 0; i < items; i++ {
			cache.StoreSeries(blockID, storage.SeriesRef(i), seriesData, tenancy.DefaultTenant)
		}
		b.ReportAllocs()
		b.ResetTimer()

		ch := make(chan int)
		for i := 0; i < 500; i++ {
			go func() {
				for range ch {
					cache.FetchMultiSeries(ctx, blockID, ids, tenancy.DefaultTenant)
				}
			}()
		}

		for i := 0; i < b.N; i++ {
			ch <- i
		}
		close(ch)
	})
}
