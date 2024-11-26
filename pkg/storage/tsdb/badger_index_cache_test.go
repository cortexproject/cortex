package tsdb

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/tenancy"
)

func TestBadgerIndexCache_UpdateItem(t *testing.T) {
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
	reg := prometheus.NewRegistry()
	cfg := BadgerIndexCacheConfig{
		DataDir:      defaultDataDir,
		EnabledItems: nil,
		GCInterval:   defaultGCInterval,
	}
	cache, err := newBadgerIndexCache(log.NewSyncLogger(errorLogger), nil, reg, cfg, defaultTTL)
	assert.NoError(t, err)
	defer cache.cleanDir(defaultDataDir)

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
			assert.Equal(t, true, ok)
			assert.Equal(t, []byte{0}, buf)
			assert.Equal(t, []string(nil), errorLogs)

			// Set the same value again.
			tt.set(0, []byte{0})
			buf, ok = tt.get(0)
			assert.Equal(t, true, ok)
			assert.Equal(t, []byte{0}, buf)
			assert.Equal(t, []string(nil), errorLogs)

			// Set a larger value.
			tt.set(1, []byte{0, 1})
			buf, ok = tt.get(1)
			assert.Equal(t, true, ok)
			assert.Equal(t, []byte{0, 1}, buf)
			assert.Equal(t, []string(nil), errorLogs)

			// Mutations to existing values will be ignored.
			tt.set(1, []byte{1, 2})
			buf, ok = tt.get(1)
			assert.Equal(t, true, ok)
			assert.Equal(t, []byte{0, 1}, buf)
			assert.Equal(t, []string(nil), errorLogs)
		})
	}
}
