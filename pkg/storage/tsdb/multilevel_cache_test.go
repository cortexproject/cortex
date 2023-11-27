package tsdb

import (
	"context"
	"sort"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/cacheutil"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
)

func Test_MultiIndexCacheInstantiation(t *testing.T) {
	multiLevelCfg := MultiLevelIndexCacheConfig{
		MaxAsyncBufferSize:  1,
		MaxAsyncConcurrency: 1,
	}
	s, err := miniredis.Run()
	if err != nil {
		testutil.Ok(t, err)
	}
	defer s.Close()

	testCases := map[string]struct {
		cfg                     IndexCacheConfig
		expectedType            storecache.IndexCache
		expectedValidationError error
	}{
		"instantiate single backends": {
			cfg: IndexCacheConfig{
				Backend: "inmemory",
			},
			expectedType: &InMemoryIndexCache{},
		},
		"instantiate multiples backends - inmemory/redis": {
			cfg: IndexCacheConfig{
				Backend: "inmemory,redis",
				Redis: RedisIndexCacheConfig{
					ClientConfig: RedisClientConfig{
						Addresses: s.Addr(),
					},
				},
				MultiLevel: multiLevelCfg,
			},
			expectedType: &multiLevelCache{},
		},
		"instantiate multiples backends - inmemory/memcached": {
			cfg: IndexCacheConfig{
				Backend: "inmemory,memcached",
				Memcached: MemcachedIndexCacheConfig{
					ClientConfig: MemcachedClientConfig{
						Addresses:           s.Addr(),
						MaxAsyncConcurrency: 1000,
					},
				},
				MultiLevel: multiLevelCfg,
			},
			expectedType: &multiLevelCache{},
		},
		"should not allow duplicate backends": {
			cfg: IndexCacheConfig{
				Backend:    "inmemory,inmemory",
				MultiLevel: multiLevelCfg,
			},
			expectedType:            &storecache.InMemoryIndexCache{},
			expectedValidationError: errDuplicatedIndexCacheBackend,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			if tc.expectedValidationError == nil {
				require.NoError(t, tc.cfg.Validate())
				c, err := NewIndexCache(tc.cfg, log.NewNopLogger(), reg)
				require.NoError(t, err)
				require.IsType(t, tc.expectedType, c)
				// Make sure we don't have any conflict with the metrics names
				_, err = cacheutil.NewMemcachedClientWithConfig(log.NewNopLogger(), "test", cacheutil.MemcachedClientConfig{MaxAsyncConcurrency: 2, Addresses: []string{s.Addr()}, DNSProviderUpdateInterval: 1}, reg)
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, tc.cfg.Validate(), errDuplicatedIndexCacheBackend)
			}
		})
	}
}

func Test_MultiLevelCache(t *testing.T) {
	cfg := MultiLevelIndexCacheConfig{
		MaxAsyncConcurrency: 10,
		MaxAsyncBufferSize:  100000,
	}
	bID, _ := ulid.Parse("01D78XZ44G0000000000000000")
	ctx := context.Background()
	l1 := labels.Label{
		Name:  "test",
		Value: "test",
	}

	l2 := labels.Label{
		Name:  "test2",
		Value: "test2",
	}

	l3 := labels.Label{
		Name:  "test3",
		Value: "test3",
	}

	matcher, err := labels.NewMatcher(labels.MatchEqual, "name", "value")
	require.NoError(t, err)

	v := make([]byte, 100)
	v2 := make([]byte, 200)

	testCases := map[string]struct {
		m1ExpectedCalls map[string][][]interface{}
		m2ExpectedCalls map[string][][]interface{}
		m1MockedCalls   map[string][]interface{}
		m2MockedCalls   map[string][]interface{}
		call            func(storecache.IndexCache)
	}{
		"[StorePostings] Should store on all caches": {
			m1ExpectedCalls: map[string][][]interface{}{
				"StorePostings": {{bID, l1, v}},
			},
			m2ExpectedCalls: map[string][][]interface{}{
				"StorePostings": {{bID, l1, v}},
			},
			call: func(cache storecache.IndexCache) {
				cache.StorePostings(bID, l1, v, "")
			},
		},
		"[StoreSeries] Should store on all caches": {
			m1ExpectedCalls: map[string][][]interface{}{
				"StoreSeries": {{bID, storage.SeriesRef(1), v}},
			},
			m2ExpectedCalls: map[string][][]interface{}{
				"StoreSeries": {{bID, storage.SeriesRef(1), v}},
			},
			call: func(cache storecache.IndexCache) {
				cache.StoreSeries(bID, 1, v, "")
			},
		},
		"[StoreExpandedPostings] Should store on all caches": {
			m1ExpectedCalls: map[string][][]interface{}{
				"StoreExpandedPostings": {{bID, []*labels.Matcher{matcher}, v}},
			},
			m2ExpectedCalls: map[string][][]interface{}{
				"StoreExpandedPostings": {{bID, []*labels.Matcher{matcher}, v}},
			},
			call: func(cache storecache.IndexCache) {
				cache.StoreExpandedPostings(bID, []*labels.Matcher{matcher}, v, "")
			},
		},
		"[FetchMultiPostings] Should fallback when all misses": {
			m1ExpectedCalls: map[string][][]interface{}{
				"FetchMultiPostings": {{bID, []labels.Label{l1, l2}}},
			},
			m2ExpectedCalls: map[string][][]interface{}{
				"FetchMultiPostings": {{bID, []labels.Label{l1, l2}}},
			},
			call: func(cache storecache.IndexCache) {
				cache.FetchMultiPostings(ctx, bID, []labels.Label{l1, l2}, "")
			},
		},
		"[FetchMultiPostings] should fallback and backfill only the missing keys on l1": {
			m1ExpectedCalls: map[string][][]interface{}{
				"FetchMultiPostings": {{bID, []labels.Label{l1, l2}}},
				"StorePostings":      {{bID, l2, v}},
			},
			m2ExpectedCalls: map[string][][]interface{}{
				"FetchMultiPostings": {{bID, []labels.Label{l2}}},
			},
			m1MockedCalls: map[string][]interface{}{
				"FetchMultiPostings": {map[labels.Label][]byte{l1: make([]byte, 1)}, []labels.Label{l2}},
			},
			m2MockedCalls: map[string][]interface{}{
				"FetchMultiPostings": {map[labels.Label][]byte{l2: v}, []labels.Label{}},
			},
			call: func(cache storecache.IndexCache) {
				cache.FetchMultiPostings(ctx, bID, []labels.Label{l1, l2}, "")
			},
		},
		"[FetchMultiPostings] should fallback and backfill only the missing keys on l1, multiple items": {
			m1ExpectedCalls: map[string][][]interface{}{
				"FetchMultiPostings": {{bID, []labels.Label{l1, l2, l3}}},
				"StorePostings":      {{bID, l2, v}, {bID, l3, v2}},
			},
			m2ExpectedCalls: map[string][][]interface{}{
				"FetchMultiPostings": {{bID, []labels.Label{l2, l3}}},
			},
			m1MockedCalls: map[string][]interface{}{
				"FetchMultiPostings": {map[labels.Label][]byte{l1: make([]byte, 1)}, []labels.Label{l2, l3}},
			},
			m2MockedCalls: map[string][]interface{}{
				"FetchMultiPostings": {map[labels.Label][]byte{l2: v, l3: v2}, []labels.Label{}},
			},
			call: func(cache storecache.IndexCache) {
				cache.FetchMultiPostings(ctx, bID, []labels.Label{l1, l2, l3}, "")
			},
		},
		"[FetchMultiPostings] should not fallback when all hit on l1": {
			m1ExpectedCalls: map[string][][]interface{}{
				"FetchMultiPostings": {{bID, []labels.Label{l1, l2}}},
			},
			m2ExpectedCalls: map[string][][]interface{}{},
			m1MockedCalls: map[string][]interface{}{
				"FetchMultiPostings": {map[labels.Label][]byte{l1: make([]byte, 1), l2: make([]byte, 1)}, []labels.Label{}},
			},
			call: func(cache storecache.IndexCache) {
				cache.FetchMultiPostings(ctx, bID, []labels.Label{l1, l2}, "")
			},
		},
		"[FetchMultiSeries] Should fallback when all misses": {
			m1ExpectedCalls: map[string][][]interface{}{
				"FetchMultiSeries": {{bID, []storage.SeriesRef{1, 2}}},
			},
			m2ExpectedCalls: map[string][][]interface{}{
				"FetchMultiSeries": {{bID, []storage.SeriesRef{1, 2}}},
			},
			call: func(cache storecache.IndexCache) {
				cache.FetchMultiSeries(ctx, bID, []storage.SeriesRef{1, 2}, "")
			},
		},
		"[FetchMultiSeries] should fallback and backfill only the missing keys on l1": {
			m1ExpectedCalls: map[string][][]interface{}{
				"FetchMultiSeries": {{bID, []storage.SeriesRef{1, 2}}},
				"StoreSeries":      {{bID, storage.SeriesRef(2), v}},
			},
			m2ExpectedCalls: map[string][][]interface{}{
				"FetchMultiSeries": {{bID, []storage.SeriesRef{2}}},
			},
			m1MockedCalls: map[string][]interface{}{
				"FetchMultiSeries": {map[storage.SeriesRef][]byte{1: v}, []storage.SeriesRef{2}},
			},
			m2MockedCalls: map[string][]interface{}{
				"FetchMultiSeries": {map[storage.SeriesRef][]byte{2: v}, []storage.SeriesRef{}},
			},
			call: func(cache storecache.IndexCache) {
				cache.FetchMultiSeries(ctx, bID, []storage.SeriesRef{1, 2}, "")
			},
		},
		"[FetchMultiSeries] should fallback and backfill only the missing keys on l1, multiple items": {
			m1ExpectedCalls: map[string][][]interface{}{
				"FetchMultiSeries": {{bID, []storage.SeriesRef{1, 2, 3}}},
				"StoreSeries": {
					{bID, storage.SeriesRef(2), v},
					{bID, storage.SeriesRef(3), v2},
				},
			},
			m2ExpectedCalls: map[string][][]interface{}{
				"FetchMultiSeries": {{bID, []storage.SeriesRef{2, 3}}},
			},
			m1MockedCalls: map[string][]interface{}{
				"FetchMultiSeries": {map[storage.SeriesRef][]byte{1: v}, []storage.SeriesRef{2, 3}},
			},
			m2MockedCalls: map[string][]interface{}{
				"FetchMultiSeries": {map[storage.SeriesRef][]byte{2: v, 3: v2}, []storage.SeriesRef{}},
			},
			call: func(cache storecache.IndexCache) {
				cache.FetchMultiSeries(ctx, bID, []storage.SeriesRef{1, 2, 3}, "")
			},
		},
		"[FetchMultiSeries] should not fallback when all hit on l1": {
			m1ExpectedCalls: map[string][][]interface{}{
				"FetchMultiSeries": {{bID, []storage.SeriesRef{1, 2}}},
			},
			m2ExpectedCalls: map[string][][]interface{}{},
			m1MockedCalls: map[string][]interface{}{
				"FetchMultiSeries": {map[storage.SeriesRef][]byte{1: make([]byte, 1), 2: make([]byte, 1)}, []storage.SeriesRef{}},
			},
			call: func(cache storecache.IndexCache) {
				cache.FetchMultiSeries(ctx, bID, []storage.SeriesRef{1, 2}, "")
			},
		},
		"[FetchExpandedPostings] Should fallback and backfill when miss": {
			m1ExpectedCalls: map[string][][]interface{}{
				"StoreExpandedPostings": {{bID, []*labels.Matcher{matcher}, v}},
				"FetchExpandedPostings": {{bID, []*labels.Matcher{matcher}}},
			},
			m2ExpectedCalls: map[string][][]interface{}{
				"FetchExpandedPostings": {{bID, []*labels.Matcher{matcher}}},
			},
			m2MockedCalls: map[string][]interface{}{
				"FetchExpandedPostings": {v, true},
			},
			call: func(cache storecache.IndexCache) {
				cache.FetchExpandedPostings(ctx, bID, []*labels.Matcher{matcher}, "")
			},
		},
		"[FetchExpandedPostings] should not fallback when all hit on l1": {
			m1ExpectedCalls: map[string][][]interface{}{
				"FetchExpandedPostings": {{bID, []*labels.Matcher{matcher}}},
			},
			m2ExpectedCalls: map[string][][]interface{}{},
			m1MockedCalls: map[string][]interface{}{
				"FetchExpandedPostings": {[]byte{}, true},
			},
			call: func(cache storecache.IndexCache) {
				cache.FetchExpandedPostings(ctx, bID, []*labels.Matcher{matcher}, "")
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			m1 := newMockIndexCache(tc.m1MockedCalls)
			m2 := newMockIndexCache(tc.m2MockedCalls)
			reg := prometheus.NewRegistry()
			c := newMultiLevelCache(reg, cfg, m1, m2)
			tc.call(c)
			mlc := c.(*multiLevelCache)
			// Wait until async operation finishes.
			mlc.backfillProcessor.Stop()
			// Sort call parameters to make test deterministic.
			for k := range m1.calls {
				switch k {
				case "StorePostings":
					sort.Slice(m1.calls[k], func(i, j int) bool {
						lbl1 := m1.calls[k][i][1].(labels.Label)
						lbl2 := m1.calls[k][j][1].(labels.Label)
						return lbl1.Name < lbl2.Name
					})
				case "StoreSeries":
					sort.Slice(m1.calls[k], func(i, j int) bool {
						seriesRef1 := m1.calls[k][i][1].(storage.SeriesRef)
						seriesRef2 := m1.calls[k][j][1].(storage.SeriesRef)
						return seriesRef1 < seriesRef2
					})
				}
			}
			require.Equal(t, tc.m1ExpectedCalls, m1.calls)
			require.Equal(t, tc.m2ExpectedCalls, m2.calls)
		})
	}
}

func newMockIndexCache(mockedCalls map[string][]interface{}) *mockIndexCache {
	return &mockIndexCache{
		calls:       map[string][][]interface{}{},
		mockedCalls: mockedCalls,
	}
}

type mockIndexCache struct {
	mtx         sync.Mutex
	calls       map[string][][]interface{}
	mockedCalls map[string][]interface{}
}

func (m *mockIndexCache) StorePostings(blockID ulid.ULID, l labels.Label, v []byte, tenant string) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.calls["StorePostings"] = append(m.calls["StorePostings"], []interface{}{blockID, l, v})
}

func (m *mockIndexCache) FetchMultiPostings(_ context.Context, blockID ulid.ULID, keys []labels.Label, tenant string) (hits map[labels.Label][]byte, misses []labels.Label) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.calls["FetchMultiPostings"] = append(m.calls["FetchMultiPostings"], []interface{}{blockID, keys})
	if m, ok := m.mockedCalls["FetchMultiPostings"]; ok {
		return m[0].(map[labels.Label][]byte), m[1].([]labels.Label)
	}

	return map[labels.Label][]byte{}, keys
}

func (m *mockIndexCache) StoreExpandedPostings(blockID ulid.ULID, matchers []*labels.Matcher, v []byte, tenant string) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.calls["StoreExpandedPostings"] = append(m.calls["StoreExpandedPostings"], []interface{}{blockID, matchers, v})
}

func (m *mockIndexCache) FetchExpandedPostings(_ context.Context, blockID ulid.ULID, matchers []*labels.Matcher, tenant string) ([]byte, bool) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.calls["FetchExpandedPostings"] = append(m.calls["FetchExpandedPostings"], []interface{}{blockID, matchers})
	if m, ok := m.mockedCalls["FetchExpandedPostings"]; ok {
		return m[0].([]byte), m[1].(bool)
	}

	return []byte{}, false
}

func (m *mockIndexCache) StoreSeries(blockID ulid.ULID, id storage.SeriesRef, v []byte, tenant string) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.calls["StoreSeries"] = append(m.calls["StoreSeries"], []interface{}{blockID, id, v})
}

func (m *mockIndexCache) FetchMultiSeries(_ context.Context, blockID ulid.ULID, ids []storage.SeriesRef, tenant string) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.calls["FetchMultiSeries"] = append(m.calls["FetchMultiSeries"], []interface{}{blockID, ids})
	if m, ok := m.mockedCalls["FetchMultiSeries"]; ok {
		return m[0].(map[storage.SeriesRef][]byte), m[1].([]storage.SeriesRef)
	}

	return map[storage.SeriesRef][]byte{}, ids
}
