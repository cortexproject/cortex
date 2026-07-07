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
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestCacheKey(t *testing.T) {
	blockID := ulid.MustNew(1, nil)
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
	r := cacheKey(blockID, matchers...)
	require.Equal(t, "00000000010000000000000000|name_1=value_1|name_2!=value_2|name_3=~value_4|name_5!~value_4|", r)
}

func Test_ShouldFetchPromiseOnlyOnce(t *testing.T) {
	cfg := PostingsCacheConfig{
		Enabled:  true,
		Ttl:      time.Hour,
		MaxBytes: 10 << 20,
	}
	m := NewPostingCacheMetrics(prometheus.NewPedanticRegistry())
	cache := newLruCache[int](cfg, "test", m, time.Now)
	calls := atomic.Int64{}
	concurrency := 100
	wg := sync.WaitGroup{}
	wg.Add(concurrency)

	fetchFunc := func() (int, int64, func() bool, error) {
		calls.Inc()
		time.Sleep(100 * time.Millisecond)
		return 0, 0, nil, nil
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
	cache := newLruCache[int](cfg, "test", m, timeNow)
	old, loaded := cache.getPromiseForKey("key1", func() (int, int64, func() bool, error) {
		return 1, 0, nil, nil
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
			cache := newLruCache[int](c.cfg, "test", m, timeNow)

			for i := range numberOfKeys {
				key := RepeatStringIfNeeded(fmt.Sprintf("key%d", i), keySize)
				p, loaded := cache.getPromiseForKey(key, func() (int, int64, func() bool, error) {
					return 1, 8, nil, nil
				})
				require.False(t, loaded)
				require.Equal(t, 1, p.v)
				require.True(t, cache.contains(key))
				p, loaded = cache.getPromiseForKey(key, func() (int, int64, func() bool, error) {
					return 1, 0, nil, nil
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
					p, loaded := cache.getPromiseForKey(key, func() (int, int64, func() bool, error) {
						return 2, 18, nil, nil
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

				cache.getPromiseForKey("newKwy", func() (int, int64, func() bool, error) {
					return 2, 18, nil, nil
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
	occurrences := map[uint32]int{}

	for range 10 {
		for j := range numberOfMetrics {
			metricName := fmt.Sprintf("metricName%v", j)
			for i := range numberOfTenants {
				userId := fmt.Sprintf("user%v", i)
				occurrences[memHashStrings(userId, metricName)]++
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
	cache := newBlocksPostingsForMatchersCache("user1", cfg, m, newLabelCounter(labelCounterArraySize))

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

func TestLruCacheEvictsLeastRecentlyUsed(t *testing.T) {
	r := prometheus.NewPedanticRegistry()
	m := NewPostingCacheMetrics(r)

	// Cache fits exactly 3 entries (each entry = 8 bytes value + 4 bytes key = 12 bytes)
	cfg := PostingsCacheConfig{
		Enabled:  true,
		Ttl:      time.Hour,
		MaxBytes: int64(3 * (8 + 4)),
	}
	cache := newLruCache[int](cfg, "test", m, time.Now)

	// Insert 3 entries: A, B, C
	cache.getPromiseForKey("aaaa", func() (int, int64, func() bool, error) { return 1, 8, nil, nil })
	cache.getPromiseForKey("bbbb", func() (int, int64, func() bool, error) { return 2, 8, nil, nil })
	cache.getPromiseForKey("cccc", func() (int, int64, func() bool, error) { return 3, 8, nil, nil })

	require.True(t, cache.contains("aaaa"))
	require.True(t, cache.contains("bbbb"))
	require.True(t, cache.contains("cccc"))

	// Access A to make it recently used (B is now least recently used)
	cache.getPromiseForKey("aaaa", func() (int, int64, func() bool, error) { return 1, 8, nil, nil })

	// Insert D — should evict B (least recently used), not A
	cache.getPromiseForKey("dddd", func() (int, int64, func() bool, error) { return 4, 8, nil, nil })

	require.True(t, cache.contains("aaaa"), "A should still be cached (recently accessed)")
	require.False(t, cache.contains("bbbb"), "B should be evicted (least recently used)")
	require.True(t, cache.contains("cccc"), "C should still be cached")
	require.True(t, cache.contains("dddd"), "D should be cached (just inserted)")
}

func BenchmarkLruCacheHitUnderPressure(b *testing.B) {
	// Simulates: 50 "hot" queries (rulers, every 30s) + many "cold" queries (ad-hoc)
	// Cache fits 100 entries. With FIFO, cold queries push out hot ones.
	// With LRU, hot queries stay cached because they're accessed frequently.

	r := prometheus.NewPedanticRegistry()
	m := NewPostingCacheMetrics(r)

	keySize := 20
	cfg := PostingsCacheConfig{
		Enabled:  true,
		Ttl:      time.Hour,
		MaxBytes: int64(100 * (8 + keySize)), // fits 100 entries
	}
	cache := newLruCache[int](cfg, "bench", m, time.Now)

	// Pre-populate with 50 hot keys
	hotKeys := make([]string, 50)
	for i := range hotKeys {
		hotKeys[i] = RepeatStringIfNeeded(fmt.Sprintf("hot-%d", i), keySize)
		cache.getPromiseForKey(hotKeys[i], func() (int, int64, func() bool, error) { return i, 8, nil, nil })
	}

	coldIdx := 0
	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		if i%3 == 0 {
			// 1/3 of accesses are hot queries (simulating ruler every 30s)
			key := hotKeys[i%len(hotKeys)]
			cache.getPromiseForKey(key, func() (int, int64, func() bool, error) { return 1, 8, nil, nil })
		} else {
			// 2/3 are cold unique queries (ad-hoc from Grafana)
			key := RepeatStringIfNeeded(fmt.Sprintf("cold-%d", coldIdx), keySize)
			coldIdx++
			cache.getPromiseForKey(key, func() (int, int64, func() bool, error) { return 1, 8, nil, nil })
		}
	}

	// Report hit rate for hot keys
	hits := 0
	for _, k := range hotKeys {
		if cache.contains(k) {
			hits++
		}
	}
	b.ReportMetric(float64(hits)/float64(len(hotKeys))*100, "%hot-retained")
}

func BenchmarkCacheGetPromise(b *testing.B) {
	r := prometheus.NewPedanticRegistry()
	m := NewPostingCacheMetrics(r)
	cfg := PostingsCacheConfig{Enabled: true, Ttl: time.Hour, MaxBytes: 10 << 20}
	cache := newLruCache[int](cfg, "bench", m, time.Now)

	// Pre-populate 1000 keys
	for i := range 1000 {
		key := fmt.Sprintf("key-%04d", i)
		cache.getPromiseForKey(key, func() (int, int64, func() bool, error) { return i, 100, nil, nil })
	}

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%04d", i%1000)
			cache.getPromiseForKey(key, func() (int, int64, func() bool, error) { return 1, 100, nil, nil })
			i++
		}
	})
}

// countingPostingsCache builds a head-enabled cache whose PostingsForMatchers records how
// many times it was actually invoked, so tests can distinguish a cache hit (fetch NOT
// re-run) from an invalidation (fetch re-run). The returned fetches pointer is bumped once
// per underlying postings lookup.
func countingPostingsCache(t *testing.T, labelCounter *labelCounter) (ExpandedPostingsCache, *atomic.Int64) {
	t.Helper()
	fetches := atomic.NewInt64(0)
	cfg := TSDBPostingsCacheConfig{
		Head: PostingsCacheConfig{
			Enabled:  true,
			Ttl:      time.Hour, // long TTL so only count invalidation can force a refetch
			MaxBytes: 10 << 20,
		},
		PostingsForMatchers: func(ctx context.Context, ix tsdb.IndexReader, ms ...*labels.Matcher) (index.Postings, error) {
			fetches.Inc()
			return index.NewListPostings([]storage.SeriesRef{1, 2, 3}), nil
		},
	}
	m := NewPostingCacheMetrics(prometheus.NewPedanticRegistry())
	cache := newBlocksPostingsForMatchersCache("user1", cfg, m, labelCounter)
	return cache, fetches
}

// query runs a head-block PostingsForMatchers and drains the result so any lazy error surfaces.
func query(t *testing.T, cache ExpandedPostingsCache, ms ...*labels.Matcher) {
	t.Helper()
	p, err := cache.PostingsForMatchers(context.Background(), headULID, nil, ms...)
	require.NoError(t, err)
	// Drain so the ListPostings is fully consumed, matching real caller behaviour.
	for p.Next() {
	}
	require.NoError(t, p.Err())
}

func TestExpandedPostingsCache_InvalidatesOnlyWhenAllTrackedCountsChange(t *testing.T) {
	labelCounter := newLabelCounter(labelCounterArraySize)
	cache, fetches := countingPostingsCache(t, labelCounter)

	nameMatcher := labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_requests")
	jobMatcher := labels.MustNewMatcher(labels.MatchEqual, "job", "api")

	// First query populates the cache.
	query(t, cache, nameMatcher, jobMatcher)
	require.Equal(t, int64(1), fetches.Load())

	// Second identical query is a pure cache hit: no new series created.
	query(t, cache, nameMatcher, jobMatcher)
	require.Equal(t, int64(1), fetches.Load(), "identical query should hit the cache")

	// A new series is created that shares the metric name but a DIFFERENT job value.
	// This bumps the __name__ count and the (metric, job) count. Because every tracked
	// count changed, the entry is invalidated and the next query refetches.
	cache.ExpireSeries(labels.FromStrings("__name__", "http_requests", "job", "web"))
	query(t, cache, nameMatcher, jobMatcher)
	require.Equal(t, int64(2), fetches.Load(), "series touching all tracked labels should invalidate")
}

func TestExpandedPostingsCache_StableLabelKeepsEntryValid(t *testing.T) {
	labelCounter := newLabelCounter(labelCounterArraySize)
	cache, fetches := countingPostingsCache(t, labelCounter)

	nameMatcher := labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_requests")
	jobMatcher := labels.MustNewMatcher(labels.MatchEqual, "job", "api")

	query(t, cache, nameMatcher, jobMatcher)
	require.Equal(t, int64(1), fetches.Load())

	// A new series is created for the SAME metric but with a label the query does not
	// track (region). This bumps __name__ and (metric, region), but NOT (metric, job).
	// Since the tracked (metric, job) count is unchanged, the entry stays valid: any
	// series matching {__name__="http_requests", job="api"} would have bumped (metric, job).
	cache.ExpireSeries(labels.FromStrings("__name__", "http_requests", "region", "us-east"))
	query(t, cache, nameMatcher, jobMatcher)
	require.Equal(t, int64(1), fetches.Load(), "unchanged tracked label should keep the entry valid")
}

func TestExpandedPostingsCache_EmptyMatchingMatcherDoesNotVouch(t *testing.T) {
	// A negative matcher like job!="batch" matches series that LACK a job label. Such a
	// series bumps __name__ but not (metric, job), so the (metric, job) count must NOT be
	// allowed to vouch for the entry. With only __name__ eligible to invalidate, any series
	// creation for the metric must force a refetch.
	labelCounter := newLabelCounter(labelCounterArraySize)
	cache, fetches := countingPostingsCache(t, labelCounter)

	nameMatcher := labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_requests")
	jobMatcher := labels.MustNewMatcher(labels.MatchNotEqual, "job", "batch")

	query(t, cache, nameMatcher, jobMatcher)
	require.Equal(t, int64(1), fetches.Load())

	// New series for the metric WITHOUT a job label — matches job!="batch". It bumps only
	// __name__ (and its other labels), not (metric, job). If the empty-matching job matcher
	// were wrongly tracked, its frozen count would keep this stale entry alive.
	cache.ExpireSeries(labels.FromStrings("__name__", "http_requests", "region", "us-east"))
	query(t, cache, nameMatcher, jobMatcher)
	require.Equal(t, int64(2), fetches.Load(), "empty-matching matcher must not keep a stale entry alive")
}

func TestExpandedPostingsCache_NameOnlyQueryInvalidatesOnAnySeriesChurn(t *testing.T) {
	// A {__name__="foo"}-only query has no selective label to track, so it degrades to the
	// old whole-metric behaviour: any series create/delete for the metric invalidates it.
	labelCounter := newLabelCounter(labelCounterArraySize)
	cache, fetches := countingPostingsCache(t, labelCounter)

	nameMatcher := labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_requests")

	query(t, cache, nameMatcher)
	require.Equal(t, int64(1), fetches.Load())

	query(t, cache, nameMatcher)
	require.Equal(t, int64(1), fetches.Load(), "identical name-only query should hit the cache")

	cache.ExpireSeries(labels.FromStrings("__name__", "http_requests", "job", "api"))
	query(t, cache, nameMatcher)
	require.Equal(t, int64(2), fetches.Load(), "name-only query must invalidate on any series churn")
}

func TestExpandedPostingsCache_DifferentMetricDoesNotInvalidate(t *testing.T) {
	// Series churn on an unrelated metric must not invalidate this metric's entry.
	labelCounter := newLabelCounter(labelCounterArraySize)
	cache, fetches := countingPostingsCache(t, labelCounter)

	nameMatcher := labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_requests")
	jobMatcher := labels.MustNewMatcher(labels.MatchEqual, "job", "api")

	query(t, cache, nameMatcher, jobMatcher)
	require.Equal(t, int64(1), fetches.Load())

	cache.ExpireSeries(labels.FromStrings("__name__", "other_metric", "job", "api"))
	query(t, cache, nameMatcher, jobMatcher)
	require.Equal(t, int64(1), fetches.Load(), "churn on a different metric must not invalidate")
}

// TestExpandedPostingsCache_SnapshotTakenBeforePostingsRead pins the ordering guarantee:
// the count snapshot MUST be captured before postings are read. If a series is created
// after the snapshot but before/around the postings read, the stored postings may miss it,
// so the stored count must be the pre-creation value and a later read must invalidate.
//
// The mock simulates that concurrent creation by bumping the metric's count from inside
// PostingsForMatchers (which runs after snapshotCounts in the correct implementation). With
// correct ordering the snapshot predates the bump, so the follow-up query refetches. If the
// snapshot were taken after the postings read, it would capture the post-bump count and the
// follow-up query would wrongly hit — failing this test.
func TestExpandedPostingsCache_SnapshotTakenBeforePostingsRead(t *testing.T) {
	labelCounter := newLabelCounter(labelCounterArraySize)
	fetches := atomic.NewInt64(0)
	bumped := atomic.NewBool(false)

	cfg := TSDBPostingsCacheConfig{
		Head: PostingsCacheConfig{
			Enabled:  true,
			Ttl:      time.Hour,
			MaxBytes: 10 << 20,
		},
		PostingsForMatchers: func(ctx context.Context, ix tsdb.IndexReader, ms ...*labels.Matcher) (index.Postings, error) {
			fetches.Inc()
			// Simulate a series for this metric being created during the fetch, once. The
			// keying matches ExpireSeries' __name__ increment: (userId, metricValue).
			if bumped.CompareAndSwap(false, true) {
				labelCounter.increment("user1", "http_requests")
			}
			return index.NewListPostings([]storage.SeriesRef{1, 2, 3}), nil
		},
	}
	m := NewPostingCacheMetrics(prometheus.NewPedanticRegistry())
	cache := newBlocksPostingsForMatchersCache("user1", cfg, m, labelCounter)

	nameMatcher := labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_requests")

	// Q1 populates the cache; the mock bumps the count after the snapshot was taken.
	query(t, cache, nameMatcher)
	require.Equal(t, int64(1), fetches.Load())

	// Q2 sees the bumped count vs the pre-bump snapshot and must refetch.
	query(t, cache, nameMatcher)
	require.Equal(t, int64(2), fetches.Load(), "snapshot must predate the postings read; a series created during the fetch must invalidate")

	// Q3: the one-shot bump is done, so the Q2 snapshot now matches the live count → hit.
	query(t, cache, nameMatcher)
	require.Equal(t, int64(2), fetches.Load(), "with no further churn the entry should be stable again")
}

func TestSnapshotCounts_Size(t *testing.T) {
	// snapshotCounts reports the byte cost that gets added to the cache entry's sizeBytes.
	// Each tracked matcher is one countSnapshot (two uint32s = 8 bytes); empty-matching
	// matchers are skipped and must contribute nothing.
	cfg := TSDBPostingsCacheConfig{
		Head: PostingsCacheConfig{Enabled: true, Ttl: time.Hour, MaxBytes: 1 << 20},
	}
	m := NewPostingCacheMetrics(prometheus.NewPedanticRegistry())
	c := newBlocksPostingsForMatchersCache("user1", cfg, m, newLabelCounter(labelCounterArraySize)).(*blocksPostingsForMatchersCache)

	name := labels.MustNewMatcher(labels.MatchEqual, "__name__", "m")
	job := labels.MustNewMatcher(labels.MatchEqual, "job", "api")
	// Empty-matching (matches a series lacking "job"), so it is skipped and adds no bytes.
	neg := labels.MustNewMatcher(labels.MatchNotEqual, "job", "batch")
	require.True(t, neg.Matches(""), "sanity: neg must be empty-matching for this test to mean anything")

	tc := map[string]struct {
		ms           []*labels.Matcher
		expectedSize int64
	}{
		"name only":                         {ms: []*labels.Matcher{name}, expectedSize: 8},
		"name + selective label":            {ms: []*labels.Matcher{name, job}, expectedSize: 16},
		"empty-matcher contributes nothing": {ms: []*labels.Matcher{name, job, neg}, expectedSize: 16},
	}

	for title, tt := range tc {
		t.Run(title, func(t *testing.T) {
			_, size := c.snapshotCounts("m", tt.ms...)
			require.Equal(t, tt.expectedSize, size)
		})
	}
}
