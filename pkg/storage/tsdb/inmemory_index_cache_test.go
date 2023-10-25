package tsdb

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
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
			typ: cacheTypePostings,
			set: func(id storage.SeriesRef, b []byte) { cache.StorePostings(uid(id), lbl, b, tenancy.DefaultTenant) },
			get: func(id storage.SeriesRef) ([]byte, bool) {
				hits, _ := cache.FetchMultiPostings(ctx, uid(id), []labels.Label{lbl}, tenancy.DefaultTenant)
				b, ok := hits[lbl]

				return b, ok
			},
		},
		{
			typ: cacheTypeSeries,
			set: func(id storage.SeriesRef, b []byte) { cache.StoreSeries(uid(id), id, b, tenancy.DefaultTenant) },
			get: func(id storage.SeriesRef) ([]byte, bool) {
				hits, _ := cache.FetchMultiSeries(ctx, uid(id), []storage.SeriesRef{id}, tenancy.DefaultTenant)
				b, ok := hits[id]

				return b, ok
			},
		},
		{
			typ: cacheTypeExpandedPostings,
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
	counter := cache.overflow.WithLabelValues(cacheTypeSeries)
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
