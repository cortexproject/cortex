package tsdb

import (
	"context"
	"errors"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

// checkLruInvariants verifies the map, the list and the byte accounting agree.
func checkLruInvariants[V any](t *testing.T, c *lruCache[V]) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()

	listLen := 0
	var prev *cacheEntryPromise[V]
	var listedBytes int64
	for e := c.front; e != nil; e = e.next {
		listLen++
		require.Same(t, prev, e.prev, "broken prev link")
		require.Same(t, e, c.cachedValues[e.key], "list entry not in map")
		if e.call.Load() == nil {
			listedBytes += e.sizeBytes
		}
		prev = e
	}
	require.Same(t, prev, c.back, "broken back pointer")
	require.Equal(t, len(c.cachedValues), listLen, "map and list sizes differ")
	require.Equal(t, listedBytes, c.cachedBytes, "cachedBytes drifted from the entries it holds")
}

func TestLruCache_ConsistentUnderConcurrency(t *testing.T) {
	tc := map[string]struct {
		keys     int
		maxBytes int64
		ttl      time.Duration
	}{
		"hot keys, expiring, full":   {keys: 50, maxBytes: 2_000, ttl: time.Millisecond},
		"hot keys, expiring, roomy":  {keys: 50, maxBytes: 1 << 30, ttl: time.Millisecond},
		"wide keys, full, no expiry": {keys: 50_000, maxBytes: 20_000, ttl: 1000 * time.Hour},
		"mixed":                      {keys: 2_000, maxBytes: 20_000, ttl: 5 * time.Millisecond},
	}
	for name, c := range tc {
		t.Run(name, func(t *testing.T) {
			var nowNs atomic.Int64
			nowNs.Store(time.Hour.Nanoseconds())
			cfg := PostingsCacheConfig{Enabled: true, MaxBytes: c.maxBytes, Ttl: c.ttl}
			cache := newLruCache[[]storage.SeriesRef](cfg, "test", NewPostingCacheMetrics(prometheus.NewPedanticRegistry()),
				func() time.Time { return time.Unix(0, nowNs.Load()) })

			stop := make(chan struct{})
			var bg sync.WaitGroup
			bg.Add(2)
			go func() { // advance the clock so entries expire while fetches are in flight
				defer bg.Done()
				for {
					select {
					case <-stop:
						return
					default:
						nowNs.Add(int64(c.ttl / 8))
						time.Sleep(50 * time.Microsecond)
					}
				}
			}()
			go func() { // periodic purge, like Ingester.expirePostingsCache
				defer bg.Done()
				for {
					select {
					case <-stop:
						return
					default:
						cache.expire()
						time.Sleep(200 * time.Microsecond)
					}
				}
			}()

			const workers, ops = 64, 5_000
			var wg sync.WaitGroup
			for w := range workers {
				wg.Add(1)
				go func(seed int64) {
					defer wg.Done()
					r := rand.New(rand.NewSource(seed))
					for range ops {
						n := 1 + r.Intn(16)
						sleep := r.Intn(4) == 0
						p, _ := cache.getPromiseForKey("key-"+strconv.Itoa(r.Intn(c.keys)), func() ([]storage.SeriesRef, int64, error) {
							if sleep {
								time.Sleep(time.Duration(r.Intn(20)) * time.Microsecond)
							}
							return make([]storage.SeriesRef, n), int64(n * 8), nil
						})
						if call := p.call.Load(); call != nil {
							<-call.done
						}
					}
				}(int64(w))
			}
			wg.Wait()
			close(stop)
			bg.Wait()

			checkLruInvariants(t, cache)
		})
	}
}

func TestLruCache_DoesNotCacheErrors(t *testing.T) {
	cfg := PostingsCacheConfig{Enabled: true, MaxBytes: 10 << 20, Ttl: time.Hour}
	cache := newLruCache[int](cfg, "test", NewPostingCacheMetrics(prometheus.NewPedanticRegistry()), time.Now)
	fetchErr := errors.New("fetch failed")

	p, loaded := cache.getPromiseForKey("key", func() (int, int64, error) { return 0, 0, fetchErr })
	require.False(t, loaded)
	require.ErrorIs(t, p.call.Load().err, fetchErr)
	require.False(t, cache.contains("key"))
	checkLruInvariants(t, cache)

	p, loaded = cache.getPromiseForKey("key", func() (int, int64, error) { return 7, 8, nil })
	require.False(t, loaded, "a failed fetch must not be served from the cache")
	require.Nil(t, p.call.Load())
	require.Equal(t, 7, p.v)
	checkLruInvariants(t, cache)
}

func TestPostingsCache_WaiterHonorsContextWhileFetchRuns(t *testing.T) {
	cfg := PostingsCacheConfig{Enabled: true, MaxBytes: 10 << 20, Ttl: time.Hour}
	c := &blocksPostingsForMatchersCache{}
	cache := newLruCache[[]storage.SeriesRef](cfg, "test", NewPostingCacheMetrics(prometheus.NewPedanticRegistry()), time.Now)

	release := make(chan struct{})
	fetched := make(chan *cacheEntryPromise[[]storage.SeriesRef])
	go func() {
		p, _ := cache.getPromiseForKey("key", func() ([]storage.SeriesRef, int64, error) {
			<-release
			return []storage.SeriesRef{1, 2}, 16, nil
		})
		fetched <- p
	}()
	require.Eventually(t, func() bool { return cache.contains("key") }, time.Second, time.Millisecond)

	waiter, loaded := cache.getPromiseForKey("key", func() ([]storage.SeriesRef, int64, error) {
		t.Fatal("concurrent caller must not fetch")
		return nil, 0, nil
	})
	require.True(t, loaded)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.result(waiter)(ctx)
	require.ErrorIs(t, err, context.Canceled, "a cancelled waiter returns without waiting for the fetch")

	close(release)
	<-fetched
	postings, err := c.result(waiter)(context.Background())
	require.NoError(t, err)
	require.True(t, postings.Next())
	require.Equal(t, storage.SeriesRef(1), postings.At())
	checkLruInvariants(t, cache)
}

// A fetch that panics, recovered by the caller as recoverIngester does in QueryStream,
// must wake callers waiting on it and must not leave the key stuck in flight.
func TestLruCache_FetchPanicDoesNotPoisonKey(t *testing.T) {
	cfg := PostingsCacheConfig{Enabled: true, MaxBytes: 10 << 20, Ttl: time.Hour}
	c := &blocksPostingsForMatchersCache{}
	cache := newLruCache[[]storage.SeriesRef](cfg, "test", NewPostingCacheMetrics(prometheus.NewPedanticRegistry()), time.Now)

	release := make(chan struct{})
	panicked := make(chan any, 1)
	go func() {
		defer func() { panicked <- recover() }()
		cache.getPromiseForKey("key", func() ([]storage.SeriesRef, int64, error) {
			<-release
			panic("boom")
		})
	}()
	require.Eventually(t, func() bool { return cache.contains("key") }, time.Second, time.Millisecond)

	waiter, loaded := cache.getPromiseForKey("key", func() ([]storage.SeriesRef, int64, error) {
		t.Fatal("concurrent caller must not fetch")
		return nil, 0, nil
	})
	require.True(t, loaded)
	close(release)
	require.Equal(t, "boom", <-panicked)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := c.result(waiter)(ctx)
	require.ErrorIs(t, err, errFetchPanicked, "a waiter on a panicked fetch must be woken, not left until its deadline")
	require.False(t, cache.contains("key"))
	checkLruInvariants(t, cache)

	var fetched atomic.Bool
	p, loaded := cache.getPromiseForKey("key", func() ([]storage.SeriesRef, int64, error) {
		fetched.Store(true)
		return []storage.SeriesRef{7}, 8, nil
	})
	require.False(t, loaded)
	require.True(t, fetched.Load(), "the next caller must fetch again")
	postings, err := c.result(p)(ctx)
	require.NoError(t, err)
	require.True(t, postings.Next())
	require.Equal(t, storage.SeriesRef(7), postings.At())
	checkLruInvariants(t, cache)
}

// BenchmarkLruCache_BytesPerEntry reports real heap bytes per cached entry for keys of
// about 190 bytes, alongside the bytes the cache accounts for.
func BenchmarkLruCache_BytesPerEntry(b *testing.B) {
	for _, ids := range []int{1, 10, 100} {
		b.Run("ids="+strconv.Itoa(ids), func(b *testing.B) {
			const entries = 100_000
			ret := make([]storage.SeriesRef, ids)
			pad := strings.Repeat("v", 120)
			fetch := func() ([]storage.SeriesRef, int64, error) {
				v := append([]storage.SeriesRef(nil), ret...)
				return v, int64(len(v) * 8), nil
			}
			cfg := PostingsCacheConfig{Enabled: true, MaxBytes: 1 << 40, Ttl: time.Hour}
			var realPerEntry, accountedPerEntry float64
			for b.Loop() {
				var before, after runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&before)
				cache := newLruCache[[]storage.SeriesRef](cfg, "bench", NewPostingCacheMetrics(nil), time.Now)
				for i := range entries {
					cache.getPromiseForKey("7|0000000000XXXXXXXRANGEHEAD|__name__=metric_"+strconv.Itoa(i%500)+"|job="+pad+strconv.Itoa(i)+"|", fetch)
				}
				runtime.GC()
				runtime.ReadMemStats(&after)
				realPerEntry = float64(after.HeapAlloc-before.HeapAlloc) / entries
				accountedPerEntry = float64(cache.cachedBytes) / entries
				runtime.KeepAlive(cache)
			}
			b.ReportMetric(realPerEntry, "heap-B/entry")
			b.ReportMetric(accountedPerEntry, "accounted-B/entry")
		})
	}
}
