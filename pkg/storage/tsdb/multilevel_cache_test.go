package tsdb

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
)

func Test_MultiIndexCacheInstantiation(t *testing.T) {
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
			expectedType: &storecache.InMemoryIndexCache{},
		},
		"instantiate multiples backends": {
			cfg: IndexCacheConfig{
				Backend: "inmemory,redis",
				Redis: RedisClientConfig{
					Addresses: s.Addr(),
				},
			},
			expectedType: newMultiLevelCache(),
		},
		"should not allow duplicate backends": {
			cfg: IndexCacheConfig{
				Backend: "inmemory,inmemory",
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
			} else {
				require.ErrorIs(t, tc.cfg.Validate(), errDuplicatedIndexCacheBackend)
			}
		})
	}
}

func Test_MultiLevelCache(t *testing.T) {
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

	matcher, err := labels.NewMatcher(labels.MatchEqual, "name", "value")
	require.NoError(t, err)

	v := make([]byte, 100)

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
				cache.StorePostings(bID, l1, v)
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
				cache.StoreSeries(bID, 1, v)
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
				cache.StoreExpandedPostings(bID, []*labels.Matcher{matcher}, v)
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
				cache.FetchMultiPostings(ctx, bID, []labels.Label{l1, l2})
			},
		},
		"[FetchMultiPostings] should fallback only the missing keys on l1": {
			m1ExpectedCalls: map[string][][]interface{}{
				"FetchMultiPostings": {{bID, []labels.Label{l1, l2}}},
			},
			m2ExpectedCalls: map[string][][]interface{}{
				"FetchMultiPostings": {{bID, []labels.Label{l2}}},
			},
			m1MockedCalls: map[string][]interface{}{
				"FetchMultiPostings": {map[labels.Label][]byte{l1: make([]byte, 1)}, []labels.Label{l2}},
			},
			call: func(cache storecache.IndexCache) {
				cache.FetchMultiPostings(ctx, bID, []labels.Label{l1, l2})
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
				cache.FetchMultiPostings(ctx, bID, []labels.Label{l1, l2})
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
				cache.FetchMultiSeries(ctx, bID, []storage.SeriesRef{1, 2})
			},
		},
		"[FetchMultiSeries] should fallback only the missing keys on l1": {
			m1ExpectedCalls: map[string][][]interface{}{
				"FetchMultiSeries": {{bID, []storage.SeriesRef{1, 2}}},
			},
			m2ExpectedCalls: map[string][][]interface{}{
				"FetchMultiSeries": {{bID, []storage.SeriesRef{2}}},
			},
			m1MockedCalls: map[string][]interface{}{
				"FetchMultiSeries": {map[storage.SeriesRef][]byte{1: make([]byte, 1)}, []storage.SeriesRef{2}},
			},
			call: func(cache storecache.IndexCache) {
				cache.FetchMultiSeries(ctx, bID, []storage.SeriesRef{1, 2})
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
				cache.FetchMultiSeries(ctx, bID, []storage.SeriesRef{1, 2})
			},
		},
		"[FetchExpandedPostings] Should fallback when miss": {
			m1ExpectedCalls: map[string][][]interface{}{
				"FetchExpandedPostings": {{bID, []*labels.Matcher{matcher}}},
			},
			m2ExpectedCalls: map[string][][]interface{}{
				"FetchExpandedPostings": {{bID, []*labels.Matcher{matcher}}},
			},
			call: func(cache storecache.IndexCache) {
				cache.FetchExpandedPostings(ctx, bID, []*labels.Matcher{matcher})
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
				cache.FetchExpandedPostings(ctx, bID, []*labels.Matcher{matcher})
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			m1 := newMockIndexCache(tc.m1MockedCalls)
			m2 := newMockIndexCache(tc.m2MockedCalls)
			c := newMultiLevelCache(m1, m2)
			tc.call(c)
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
	calls       map[string][][]interface{}
	mockedCalls map[string][]interface{}
}

func (m *mockIndexCache) StorePostings(blockID ulid.ULID, l labels.Label, v []byte) {
	m.calls["StorePostings"] = append(m.calls["StorePostings"], []interface{}{blockID, l, v})
}

func (m *mockIndexCache) FetchMultiPostings(_ context.Context, blockID ulid.ULID, keys []labels.Label) (hits map[labels.Label][]byte, misses []labels.Label) {
	m.calls["FetchMultiPostings"] = append(m.calls["FetchMultiPostings"], []interface{}{blockID, keys})
	if m, ok := m.mockedCalls["FetchMultiPostings"]; ok {
		return m[0].(map[labels.Label][]byte), m[1].([]labels.Label)
	}

	return map[labels.Label][]byte{}, keys
}

func (m *mockIndexCache) StoreExpandedPostings(blockID ulid.ULID, matchers []*labels.Matcher, v []byte) {
	m.calls["StoreExpandedPostings"] = append(m.calls["StoreExpandedPostings"], []interface{}{blockID, matchers, v})
}

func (m *mockIndexCache) FetchExpandedPostings(_ context.Context, blockID ulid.ULID, matchers []*labels.Matcher) ([]byte, bool) {
	m.calls["FetchExpandedPostings"] = append(m.calls["FetchExpandedPostings"], []interface{}{blockID, matchers})
	if m, ok := m.mockedCalls["FetchExpandedPostings"]; ok {
		return m[0].([]byte), m[1].(bool)
	}

	return []byte{}, false
}

func (m *mockIndexCache) StoreSeries(blockID ulid.ULID, id storage.SeriesRef, v []byte) {
	m.calls["StoreSeries"] = append(m.calls["StoreSeries"], []interface{}{blockID, id, v})
}

func (m *mockIndexCache) FetchMultiSeries(_ context.Context, blockID ulid.ULID, ids []storage.SeriesRef) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef) {
	m.calls["FetchMultiSeries"] = append(m.calls["FetchMultiSeries"], []interface{}{blockID, ids})
	if m, ok := m.mockedCalls["FetchMultiSeries"]; ok {
		return m[0].(map[storage.SeriesRef][]byte), m[1].([]storage.SeriesRef)
	}

	return map[storage.SeriesRef][]byte{}, ids
}
