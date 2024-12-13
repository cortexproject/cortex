package tsdb

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/cache"
)

func Test_MultiLevelChunkCacheStore(t *testing.T) {
	ttl := time.Hour * 24
	cfg := MultiLevelChunkCacheConfig{
		MaxAsyncConcurrency: 10,
		MaxAsyncBufferSize:  100000,
		MaxBackfillItems:    10000,
		BackFillTTL:         ttl,
	}

	data := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	}

	testCases := map[string]struct {
		m1InitData     map[string][]byte
		m2InitData     map[string][]byte
		expectedM1Data map[string][]byte
		expectedM2Data map[string][]byte
		storeData      map[string][]byte
	}{
		"should stored data to both caches": {
			m1InitData:     nil,
			m2InitData:     nil,
			expectedM1Data: data,
			expectedM2Data: data,
			storeData:      data,
		},
		"should stored data to m1 cache": {
			m1InitData:     nil,
			m2InitData:     data,
			expectedM1Data: data,
			expectedM2Data: data,
			storeData:      data,
		},
		"should stored data to m2 cache": {
			m1InitData:     data,
			m2InitData:     nil,
			expectedM1Data: data,
			expectedM2Data: data,
			storeData:      data,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			m1 := newMockChunkCache("m1", tc.m1InitData)
			m2 := newMockChunkCache("m2", tc.m2InitData)
			reg := prometheus.NewRegistry()
			c := newMultiLevelChunkCache("chunk-cache", cfg, reg, m1, m2)
			c.Store(tc.storeData, ttl)

			mlc := c.(*multiLevelChunkCache)
			// Wait until async operation finishes.
			mlc.backfillProcessor.Stop()

			require.Equal(t, tc.expectedM1Data, m1.data)
			require.Equal(t, tc.expectedM2Data, m2.data)
		})
	}
}

func Test_MultiLevelChunkCacheFetchRace(t *testing.T) {
	cfg := MultiLevelChunkCacheConfig{
		MaxAsyncConcurrency: 10,
		MaxAsyncBufferSize:  100000,
		MaxBackfillItems:    10000,
		BackFillTTL:         time.Hour * 24,
	}
	reg := prometheus.NewRegistry()

	m1 := newMockChunkCache("m1", map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
	})

	inMemory, err := cache.NewInMemoryCacheWithConfig("test", log.NewNopLogger(), reg, cache.InMemoryCacheConfig{MaxSize: 10 * 1024, MaxItemSize: 1024})
	require.NoError(t, err)

	inMemory.Store(map[string][]byte{
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	}, time.Minute)

	c := newMultiLevelChunkCache("chunk-cache", cfg, reg, inMemory, m1)

	hits := c.Fetch(context.Background(), []string{"key1", "key2", "key3", "key4"})

	require.Equal(t, 3, len(hits))

	// We should be able to change the returned values without any race problem
	delete(hits, "key1")

	mlc := c.(*multiLevelChunkCache)
	//Wait until async operation finishes.
	mlc.backfillProcessor.Stop()

}

func Test_MultiLevelChunkCacheFetch(t *testing.T) {
	cfg := MultiLevelChunkCacheConfig{
		MaxAsyncConcurrency: 10,
		MaxAsyncBufferSize:  100000,
		MaxBackfillItems:    10000,
		BackFillTTL:         time.Hour * 24,
	}

	testCases := map[string]struct {
		m1ExistingData        map[string][]byte
		m2ExistingData        map[string][]byte
		expectedM1Data        map[string][]byte
		expectedM2Data        map[string][]byte
		expectedFetchedData   map[string][]byte
		expectedM1FetchedKeys []string
		expectedM2FetchedKeys []string
		fetchKeys             []string
	}{
		"fetched data should be union of m1, m2 and 'key2' and `key3' should be backfilled to m1": {
			m1ExistingData: map[string][]byte{
				"key1": []byte("value1"),
			},
			m2ExistingData: map[string][]byte{
				"key2": []byte("value2"),
				"key3": []byte("value3"),
			},
			expectedM1FetchedKeys: []string{"key1", "key2", "key3"},
			expectedM2FetchedKeys: []string{"key2", "key3"},
			expectedM1Data: map[string][]byte{
				"key1": []byte("value1"),
				"key2": []byte("value2"),
				"key3": []byte("value3"),
			},
			expectedM2Data: map[string][]byte{
				"key2": []byte("value2"),
				"key3": []byte("value3"),
			},
			expectedFetchedData: map[string][]byte{
				"key1": []byte("value1"),
				"key2": []byte("value2"),
				"key3": []byte("value3"),
			},
			fetchKeys: []string{"key1", "key2", "key3"},
		},
		"should be not fetched data that do not exist in both caches": {
			m1ExistingData: map[string][]byte{
				"key1": []byte("value1"),
			},
			m2ExistingData: map[string][]byte{
				"key2": []byte("value2"),
			},
			expectedM1FetchedKeys: []string{"key1", "key2", "key3"},
			expectedM2FetchedKeys: []string{"key2", "key3"},
			expectedM1Data: map[string][]byte{
				"key1": []byte("value1"),
				"key2": []byte("value2"),
			},
			expectedM2Data: map[string][]byte{
				"key2": []byte("value2"),
			},
			expectedFetchedData: map[string][]byte{
				"key1": []byte("value1"),
				"key2": []byte("value2"),
			},
			fetchKeys: []string{"key1", "key2", "key3"},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			m1 := newMockChunkCache("m1", tc.m1ExistingData)
			m2 := newMockChunkCache("m2", tc.m2ExistingData)
			reg := prometheus.NewRegistry()
			c := newMultiLevelChunkCache("chunk-cache", cfg, reg, m1, m2)
			fetchData := c.Fetch(context.Background(), tc.fetchKeys)

			mlc := c.(*multiLevelChunkCache)
			// Wait until async operation finishes.
			mlc.backfillProcessor.Stop()

			require.Equal(t, tc.expectedM1Data, m1.data)
			require.Equal(t, tc.expectedM2Data, m2.data)
			require.Equal(t, tc.expectedFetchedData, fetchData)
		})
	}
}

type mockChunkCache struct {
	mu   sync.Mutex
	name string
	data map[string][]byte

	fetchedKeys []string
}

func newMockChunkCache(name string, data map[string][]byte) *mockChunkCache {
	if data == nil {
		data = make(map[string][]byte)
	}

	return &mockChunkCache{
		name: name,
		data: data,
	}
}

func (m *mockChunkCache) Store(data map[string][]byte, _ time.Duration) {
	m.data = data
}

func (m *mockChunkCache) Fetch(_ context.Context, keys []string) map[string][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	h := map[string][]byte{}

	for _, k := range keys {
		m.fetchedKeys = append(m.fetchedKeys, k)
		if _, ok := m.data[k]; ok {
			h[k] = m.data[k]
		}
	}

	return h
}

func (m *mockChunkCache) Name() string {
	return m.name
}
