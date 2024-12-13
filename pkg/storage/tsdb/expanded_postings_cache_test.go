package tsdb

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func Test_ShouldFetchPromiseOnlyOnce(t *testing.T) {
	cfg := PostingsCacheConfig{
		Enabled:  true,
		Ttl:      time.Hour,
		MaxBytes: 10 << 20,
	}
	m := NewPostingCacheMetrics(prometheus.NewPedanticRegistry())
	cache := newFifoCache[int](cfg, "test", m, time.Now)
	calls := atomic.Int64{}
	concurrency := 100
	wg := sync.WaitGroup{}
	wg.Add(concurrency)

	fetchFunc := func() (int, int64, error) {
		calls.Inc()
		time.Sleep(100 * time.Millisecond)
		return 0, 0, nil
	}

	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			cache.getPromiseForKey("key1", fetchFunc)
		}()
	}

	wg.Wait()
	require.Equal(t, int64(1), calls.Load())

}

func TestFifoCacheDisabled(t *testing.T) {
	cfg := PostingsCacheConfig{}
	cfg.Enabled = false
	m := NewPostingCacheMetrics(prometheus.NewPedanticRegistry())
	timeNow := time.Now
	cache := newFifoCache[int](cfg, "test", m, timeNow)
	old, loaded := cache.getPromiseForKey("key1", func() (int, int64, error) {
		return 1, 0, nil
	})
	require.False(t, loaded)
	require.Equal(t, 1, old.v)
	require.False(t, cache.contains("key1"))
}

func TestFifoCacheExpire(t *testing.T) {

	keySize := 20
	numberOfKeys := 100

	tc := map[string]struct {
		cfg                PostingsCacheConfig
		expectedFinalItems int
		ttlExpire          bool
	}{
		"MaxBytes": {
			expectedFinalItems: 10,
			cfg: PostingsCacheConfig{
				Enabled:  true,
				Ttl:      time.Hour,
				MaxBytes: int64(10 * (8 + keySize)),
			},
		},
		"TTL": {
			expectedFinalItems: numberOfKeys,
			ttlExpire:          true,
			cfg: PostingsCacheConfig{
				Enabled:  true,
				Ttl:      time.Hour,
				MaxBytes: 10 << 20,
			},
		},
	}

	for name, c := range tc {
		t.Run(name, func(t *testing.T) {
			r := prometheus.NewPedanticRegistry()
			m := NewPostingCacheMetrics(r)
			timeNow := time.Now
			cache := newFifoCache[int](c.cfg, "test", m, timeNow)

			for i := 0; i < numberOfKeys; i++ {
				key := RepeatStringIfNeeded(fmt.Sprintf("key%d", i), keySize)
				p, loaded := cache.getPromiseForKey(key, func() (int, int64, error) {
					return 1, 8, nil
				})
				require.False(t, loaded)
				require.Equal(t, 1, p.v)
				require.True(t, cache.contains(key))
				p, loaded = cache.getPromiseForKey(key, func() (int, int64, error) {
					return 1, 0, nil
				})
				require.True(t, loaded)
				require.Equal(t, 1, p.v)
			}

			totalCacheSize := 0

			for i := 0; i < numberOfKeys; i++ {
				key := RepeatStringIfNeeded(fmt.Sprintf("key%d", i), keySize)
				if cache.contains(key) {
					totalCacheSize++
				}
			}

			require.Equal(t, c.expectedFinalItems, totalCacheSize)

			if c.expectedFinalItems != numberOfKeys {
				err := testutil.GatherAndCompare(r, bytes.NewBufferString(fmt.Sprintf(`
		# HELP cortex_ingester_expanded_postings_cache_evicts Total number of evictions in the cache, excluding items that got evicted due to TTL.
		# TYPE cortex_ingester_expanded_postings_cache_evicts counter
        cortex_ingester_expanded_postings_cache_evicts{cache="test",reason="full"} %v
`, numberOfKeys-c.expectedFinalItems)), "cortex_ingester_expanded_postings_cache_evicts")
				require.NoError(t, err)

			}

			if c.ttlExpire {
				cache.timeNow = func() time.Time {
					return timeNow().Add(2 * c.cfg.Ttl)
				}

				for i := 0; i < numberOfKeys; i++ {
					key := RepeatStringIfNeeded(fmt.Sprintf("key%d", i), keySize)
					originalSize := cache.cachedBytes
					p, loaded := cache.getPromiseForKey(key, func() (int, int64, error) {
						return 2, 18, nil
					})
					require.False(t, loaded)
					// New value
					require.Equal(t, 2, p.v)
					// Total Size Updated
					require.Equal(t, originalSize+10, cache.cachedBytes)
				}

				err := testutil.GatherAndCompare(r, bytes.NewBufferString(fmt.Sprintf(`
		# HELP cortex_ingester_expanded_postings_cache_evicts Total number of evictions in the cache, excluding items that got evicted due to TTL.
		# TYPE cortex_ingester_expanded_postings_cache_evicts counter
        cortex_ingester_expanded_postings_cache_evicts{cache="test",reason="expired"} %v
`, numberOfKeys)), "cortex_ingester_expanded_postings_cache_evicts")
				require.NoError(t, err)

				cache.timeNow = func() time.Time {
					return timeNow().Add(5 * c.cfg.Ttl)
				}

				cache.getPromiseForKey("newKwy", func() (int, int64, error) {
					return 2, 18, nil
				})

				// Should expire all keys again as ttl is expired
				err = testutil.GatherAndCompare(r, bytes.NewBufferString(fmt.Sprintf(`
		# HELP cortex_ingester_expanded_postings_cache_evicts Total number of evictions in the cache, excluding items that got evicted due to TTL.
		# TYPE cortex_ingester_expanded_postings_cache_evicts counter
        cortex_ingester_expanded_postings_cache_evicts{cache="test",reason="expired"} %v
`, numberOfKeys*2)), "cortex_ingester_expanded_postings_cache_evicts")
				require.NoError(t, err)
			}
		})
	}
}

func Test_memHashString(test *testing.T) {
	numberOfTenants := 200
	numberOfMetrics := 100
	occurrences := map[uint64]int{}

	for k := 0; k < 10; k++ {
		for j := 0; j < numberOfMetrics; j++ {
			metricName := fmt.Sprintf("metricName%v", j)
			for i := 0; i < numberOfTenants; i++ {
				userId := fmt.Sprintf("user%v", i)
				occurrences[memHashString(userId, metricName)]++
			}
		}

		require.Len(test, occurrences, numberOfMetrics*numberOfTenants)
	}
}

func RepeatStringIfNeeded(seed string, length int) string {
	if len(seed) > length {
		return seed
	}

	return strings.Repeat(seed, 1+length/len(seed))[:max(length, len(seed))]
}
