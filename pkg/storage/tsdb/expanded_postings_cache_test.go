package tsdb

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestCacheKey(t *testing.T) {
	blockID := ulid.MustNew(1, nil)
	seed := "seed123"
	matchers := []*labels.Matcher{
		{
			Type:  labels.MatchEqual,
			Name:  "name_1",
			Value: "value_1",
		},
		{
			Type:  labels.MatchNotEqual,
			Name:  "name_2",
			Value: "value_2",
		},
		{
			Type:  labels.MatchRegexp,
			Name:  "name_3",
			Value: "value_4",
		},
		{
			Type:  labels.MatchNotRegexp,
			Name:  "name_5",
			Value: "value_4",
		},
	}
	r := cacheKey(seed, blockID, matchers...)
	require.Equal(t, "seed123|00000000010000000000000000|name_1=value_1|name_2!=value_2|name_3=~value_4|name_5!~value_4|", r)
}

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

	for range 100 {
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

			for i := range numberOfKeys {
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

			for i := range numberOfKeys {
				key := RepeatStringIfNeeded(fmt.Sprintf("key%d", i), keySize)
				if cache.contains(key) {
					totalCacheSize++
				}
			}

			require.Equal(t, c.expectedFinalItems, totalCacheSize)

			if c.expectedFinalItems != numberOfKeys {
				err := testutil.GatherAndCompare(r, bytes.NewBufferString(fmt.Sprintf(`
		# HELP cortex_ingester_expanded_postings_cache_evicts_total Total number of evictions in the cache, excluding items that got evicted due to TTL.
		# TYPE cortex_ingester_expanded_postings_cache_evicts_total counter
        cortex_ingester_expanded_postings_cache_evicts_total{cache="test",reason="full"} %v
`, numberOfKeys-c.expectedFinalItems)), "cortex_ingester_expanded_postings_cache_evicts_total")
				require.NoError(t, err)

			}

			if c.ttlExpire {
				cache.timeNow = func() time.Time {
					return timeNow().Add(2 * c.cfg.Ttl)
				}

				for i := range numberOfKeys {
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
		# HELP cortex_ingester_expanded_postings_cache_miss_total Total number of miss requests to the cache.
		# TYPE cortex_ingester_expanded_postings_cache_miss_total counter
		cortex_ingester_expanded_postings_cache_miss_total{cache="test",reason="expired"} %v
		cortex_ingester_expanded_postings_cache_miss_total{cache="test",reason="miss"} %v
`, numberOfKeys, numberOfKeys)), "cortex_ingester_expanded_postings_cache_miss_total")
				require.NoError(t, err)

				cache.timeNow = func() time.Time {
					return timeNow().Add(5 * c.cfg.Ttl)
				}

				cache.getPromiseForKey("newKwy", func() (int, int64, error) {
					return 2, 18, nil
				})

				// Should expire all keys expired keys
				err = testutil.GatherAndCompare(r, bytes.NewBufferString(fmt.Sprintf(`
		# HELP cortex_ingester_expanded_postings_cache_evicts_total Total number of evictions in the cache, excluding items that got evicted due to TTL.
		# TYPE cortex_ingester_expanded_postings_cache_evicts_total counter
        cortex_ingester_expanded_postings_cache_evicts_total{cache="test",reason="expired"} %v
`, numberOfKeys)), "cortex_ingester_expanded_postings_cache_evicts_total")
				require.NoError(t, err)
			}
		})
	}
}

func Test_memHashString(test *testing.T) {
	numberOfTenants := 200
	numberOfMetrics := 100
	occurrences := map[uint64]int{}

	for range 10 {
		for j := range numberOfMetrics {
			metricName := fmt.Sprintf("metricName%v", j)
			for i := range numberOfTenants {
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

func TestPostingsCacheFetchTimeout(t *testing.T) {
	// Test that the fetch operation respects the FetchTimeout configuration
	// to prevent runaway queries when all callers have given up.
	cfg := TSDBPostingsCacheConfig{
		Head: PostingsCacheConfig{
			Enabled:      true,
			Ttl:          time.Hour,
			MaxBytes:     10 << 20,
			FetchTimeout: 100 * time.Millisecond,
		},
	}

	fetchStarted := make(chan struct{})
	fetchShouldBlock := make(chan struct{})
	fetchCompleted := atomic.Bool{}

	cfg.PostingsForMatchers = func(ctx context.Context, ix tsdb.IndexReader, ms ...*labels.Matcher) (index.Postings, error) {
		close(fetchStarted)
		select {
		case <-ctx.Done():
			// Good! Context was cancelled due to timeout
			return nil, ctx.Err()
		case <-fetchShouldBlock:
			// This shouldn't happen - the fetch should be cancelled by timeout
			fetchCompleted.Store(true)
			return index.EmptyPostings(), nil
		}
	}

	m := NewPostingCacheMetrics(prometheus.NewPedanticRegistry())
	cache := newBlocksPostingsForMatchersCache("user1", cfg, m, newSeedByHash(seedArraySize))

	// Start a query that will trigger the fetch
	blockID := headULID
	queryCtx, queryCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer queryCancel()

	_, err := cache.PostingsForMatchers(queryCtx, blockID, nil, labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric"))

	// Wait for fetch to start
	<-fetchStarted

	// The query context will timeout after 50ms
	// But the fetch should continue with its own timeout (100ms)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// Wait a bit more than the fetch timeout
	time.Sleep(1 * time.Second)

	// The fetch should have been cancelled by its timeout, not completed
	require.False(t, fetchCompleted.Load(), "Fetch should have been cancelled by timeout, not completed")

	close(fetchShouldBlock)
}
