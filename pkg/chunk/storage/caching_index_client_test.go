package storage

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/cache"
)

var ctx = user.InjectOrgID(context.Background(), "1")

type mockStore struct {
	chunk.IndexClient
	queries int
	results ReadBatch
}

func (m *mockStore) QueryPages(ctx context.Context, queries []chunk.IndexQuery, callback func(chunk.IndexQuery, chunk.ReadBatch) (shouldContinue bool)) error {
	for _, query := range queries {
		m.queries++
		callback(query, m.results)
	}
	return nil
}

func TestCachingStorageClientBasic(t *testing.T) {
	store := &mockStore{
		results: ReadBatch{
			Entries: []Entry{{
				Column: []byte("foo"),
				Value:  []byte("bar"),
			}},
		},
	}
	limits, err := defaultLimits()
	require.NoError(t, err)
	logger := log.NewNopLogger()
	cache := cache.NewFifoCache("test", cache.FifoCacheConfig{MaxSizeItems: 10, Validity: 10 * time.Second}, nil, logger)
	client := newCachingIndexClient(store, cache, 1*time.Second, limits, logger)
	queries := []chunk.IndexQuery{{
		TableName: "table",
		HashValue: "baz",
	}}
	err = client.QueryPages(ctx, queries, func(_ chunk.IndexQuery, _ chunk.ReadBatch) bool {
		return true
	})
	require.NoError(t, err)
	assert.EqualValues(t, 1, store.queries)

	// If we do the query to the cache again, the underlying store shouldn't see it.
	err = client.QueryPages(ctx, queries, func(_ chunk.IndexQuery, _ chunk.ReadBatch) bool {
		return true
	})
	require.NoError(t, err)
	assert.EqualValues(t, 1, store.queries)
}

func TestTempCachingStorageClient(t *testing.T) {
	store := &mockStore{
		results: ReadBatch{
			Entries: []Entry{{
				Column: []byte("foo"),
				Value:  []byte("bar"),
			}},
		},
	}
	limits, err := defaultLimits()
	require.NoError(t, err)
	logger := log.NewNopLogger()
	cache := cache.NewFifoCache("test", cache.FifoCacheConfig{MaxSizeItems: 10, Validity: 10 * time.Second}, nil, logger)
	client := newCachingIndexClient(store, cache, 100*time.Millisecond, limits, logger)
	queries := []chunk.IndexQuery{
		{TableName: "table", HashValue: "foo"},
		{TableName: "table", HashValue: "bar"},
		{TableName: "table", HashValue: "baz"},
	}
	results := 0
	err = client.QueryPages(ctx, queries, func(query chunk.IndexQuery, batch chunk.ReadBatch) bool {
		iter := batch.Iterator()
		for iter.Next() {
			results++
		}
		return true
	})
	require.NoError(t, err)
	assert.EqualValues(t, len(queries), store.queries)
	assert.EqualValues(t, len(queries), results)

	// If we do the query to the cache again, the underlying store shouldn't see it.
	results = 0
	err = client.QueryPages(ctx, queries, func(query chunk.IndexQuery, batch chunk.ReadBatch) bool {
		iter := batch.Iterator()
		for iter.Next() {
			results++
		}
		return true
	})
	require.NoError(t, err)
	assert.EqualValues(t, len(queries), store.queries)
	assert.EqualValues(t, len(queries), results)

	// If we do the query after validity, it should see the queries.
	time.Sleep(100 * time.Millisecond)
	results = 0
	err = client.QueryPages(ctx, queries, func(query chunk.IndexQuery, batch chunk.ReadBatch) bool {
		iter := batch.Iterator()
		for iter.Next() {
			results++
		}
		return true
	})
	require.NoError(t, err)
	assert.EqualValues(t, 2*len(queries), store.queries)
	assert.EqualValues(t, len(queries), results)
}

func TestPermCachingStorageClient(t *testing.T) {
	store := &mockStore{
		results: ReadBatch{
			Entries: []Entry{{
				Column: []byte("foo"),
				Value:  []byte("bar"),
			}},
		},
	}
	limits, err := defaultLimits()
	require.NoError(t, err)
	logger := log.NewNopLogger()
	cache := cache.NewFifoCache("test", cache.FifoCacheConfig{MaxSizeItems: 10, Validity: 10 * time.Second}, nil, logger)
	client := newCachingIndexClient(store, cache, 100*time.Millisecond, limits, logger)
	queries := []chunk.IndexQuery{
		{TableName: "table", HashValue: "foo", Immutable: true},
		{TableName: "table", HashValue: "bar", Immutable: true},
		{TableName: "table", HashValue: "baz", Immutable: true},
	}
	results := 0
	err = client.QueryPages(ctx, queries, func(query chunk.IndexQuery, batch chunk.ReadBatch) bool {
		iter := batch.Iterator()
		for iter.Next() {
			results++
		}
		return true
	})
	require.NoError(t, err)
	assert.EqualValues(t, len(queries), store.queries)
	assert.EqualValues(t, len(queries), results)

	// If we do the query to the cache again, the underlying store shouldn't see it.
	results = 0
	err = client.QueryPages(ctx, queries, func(query chunk.IndexQuery, batch chunk.ReadBatch) bool {
		iter := batch.Iterator()
		for iter.Next() {
			results++
		}
		return true
	})
	require.NoError(t, err)
	assert.EqualValues(t, len(queries), store.queries)
	assert.EqualValues(t, len(queries), results)

	// If we do the query after validity, it still shouldn't see the queries.
	time.Sleep(200 * time.Millisecond)
	results = 0
	err = client.QueryPages(ctx, queries, func(query chunk.IndexQuery, batch chunk.ReadBatch) bool {
		iter := batch.Iterator()
		for iter.Next() {
			results++
		}
		return true
	})
	require.NoError(t, err)
	assert.EqualValues(t, len(queries), store.queries)
	assert.EqualValues(t, len(queries), results)
}

func TestCachingStorageClientEmptyResponse(t *testing.T) {
	store := &mockStore{}
	limits, err := defaultLimits()
	require.NoError(t, err)
	logger := log.NewNopLogger()
	cache := cache.NewFifoCache("test", cache.FifoCacheConfig{MaxSizeItems: 10, Validity: 10 * time.Second}, nil, logger)
	client := newCachingIndexClient(store, cache, 1*time.Second, limits, logger)
	queries := []chunk.IndexQuery{{TableName: "table", HashValue: "foo"}}
	err = client.QueryPages(ctx, queries, func(query chunk.IndexQuery, batch chunk.ReadBatch) bool {
		assert.False(t, batch.Iterator().Next())
		return true
	})
	require.NoError(t, err)
	assert.EqualValues(t, 1, store.queries)

	// If we do the query to the cache again, the underlying store shouldn't see it.
	err = client.QueryPages(ctx, queries, func(query chunk.IndexQuery, batch chunk.ReadBatch) bool {
		assert.False(t, batch.Iterator().Next())
		return true
	})
	require.NoError(t, err)
	assert.EqualValues(t, 1, store.queries)
}

func TestCachingStorageClientStoreQueries(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		queries              []chunk.IndexQuery
		expectedStoreQueries int
	}{
		{
			name: "TableName-HashValue queries",
			queries: []chunk.IndexQuery{
				{TableName: "table", HashValue: "foo"},
				{TableName: "table", HashValue: "bar"},
			},
			expectedStoreQueries: 2,
		},
		{
			name: "TableName-HashValue-RangeValuePrefix queries",
			queries: []chunk.IndexQuery{
				{TableName: "table", HashValue: "foo", RangeValuePrefix: []byte("bar")},
				{TableName: "table", HashValue: "foo", RangeValuePrefix: []byte("baz")},
				{TableName: "table", HashValue: "foo", RangeValuePrefix: []byte("taz")},
			},
			expectedStoreQueries: 3,
		},
		{
			name: "TableName-HashValue-RangeValuePrefix-ValueEqual queries",
			queries: []chunk.IndexQuery{
				{TableName: "table", HashValue: "foo", RangeValuePrefix: []byte("bar"), ValueEqual: []byte("one")},
				{TableName: "table", HashValue: "foo", RangeValuePrefix: []byte("baz"), ValueEqual: []byte("two")},
				{TableName: "table", HashValue: "foo", RangeValuePrefix: []byte("taz"), ValueEqual: []byte("three")},
			},
			expectedStoreQueries: 3,
		},
		{
			name: "TableName-HashValue-RangeValueStart queries",
			queries: []chunk.IndexQuery{
				{TableName: "table", HashValue: "foo", RangeValueStart: []byte("bar")},
				{TableName: "table", HashValue: "foo", RangeValueStart: []byte("baz")},
			},
			expectedStoreQueries: 1,
		},
		{
			name: "Duplicate queries",
			queries: []chunk.IndexQuery{
				{TableName: "table", HashValue: "foo"},
				{TableName: "table", HashValue: "foo"},
				{TableName: "table", HashValue: "foo", RangeValuePrefix: []byte("bar")},
				{TableName: "table", HashValue: "foo", RangeValuePrefix: []byte("bar")},
				{TableName: "table", HashValue: "foo", RangeValuePrefix: []byte("bar"), ValueEqual: []byte("one")},
				{TableName: "table", HashValue: "foo", RangeValuePrefix: []byte("bar"), ValueEqual: []byte("one")},
			},
			expectedStoreQueries: 3,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &mockStore{
				results: ReadBatch{
					Entries: []Entry{
						{
							Column: []byte("bar"),
							Value:  []byte("bar"),
						},
						{
							Column: []byte("baz"),
							Value:  []byte("baz"),
						},
					},
				},
			}
			limits, err := defaultLimits()
			require.NoError(t, err)
			logger := log.NewNopLogger()
			cache := cache.NewFifoCache("test", cache.FifoCacheConfig{MaxSizeItems: 10, Validity: 10 * time.Second}, nil, logger)
			client := newCachingIndexClient(store, cache, 1*time.Second, limits, logger)

			err = client.QueryPages(ctx, tc.queries, func(query chunk.IndexQuery, batch chunk.ReadBatch) bool {
				return true
			})
			require.NoError(t, err)
			assert.EqualValues(t, tc.expectedStoreQueries, store.queries)

			// If we do the query to the cache again, the underlying store shouldn't see it.
			err = client.QueryPages(ctx, tc.queries, func(query chunk.IndexQuery, batch chunk.ReadBatch) bool {
				return true
			})
			require.NoError(t, err)
			assert.EqualValues(t, tc.expectedStoreQueries, store.queries)
		})
	}
}

func TestQueryKey(t *testing.T) {
	testTableName := "test"
	testHashValue := "hash"
	testRangeValuePrefix := []byte("prefix")
	testRangeValueStart := []byte("start")
	testValueEqual := []byte("equal")

	for _, tc := range []struct {
		name     string
		query    chunk.IndexQuery
		expected string
	}{
		{
			name: "TableName-HashValue query",
			query: chunk.IndexQuery{
				TableName: testTableName,
				HashValue: testHashValue,
			},
			expected: testTableName + sep + testHashValue,
		},
		{
			name: "TableName-HashValue-RangeValuePrefix query",
			query: chunk.IndexQuery{
				TableName:        testTableName,
				HashValue:        testHashValue,
				RangeValuePrefix: testRangeValuePrefix,
			},
			expected: testTableName + sep + testHashValue + sep + yoloString(testRangeValuePrefix),
		},
		{
			name: "TableName-HashValue-RangeValuePrefix-ValueEqual query",
			query: chunk.IndexQuery{
				TableName:        testTableName,
				HashValue:        testHashValue,
				RangeValuePrefix: testRangeValuePrefix,
				ValueEqual:       testValueEqual,
			},
			expected: testTableName + sep + testHashValue + sep + yoloString(testRangeValuePrefix) + sep + yoloString(testValueEqual),
		},
		{
			name: "TableName-HashValue-RangeValueStart query",
			query: chunk.IndexQuery{
				TableName:       testTableName,
				HashValue:       testHashValue,
				RangeValueStart: testRangeValueStart,
			},
			expected: testTableName + sep + testHashValue,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, queryKey(tc.query))
		})
	}

}
