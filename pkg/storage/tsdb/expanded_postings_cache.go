package tsdb

import (
	"context"
	"errors"
	"flag"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/segmentio/fasthash/fnv1a"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/util/extract"
	logutil "github.com/cortexproject/cortex/pkg/util/log"
)

var (
	rangeHeadULID = ulid.MustParse("0000000000XXXXXXXRANGEHEAD")
	headULID      = ulid.MustParse("0000000000XXXXXXXXXXXXHEAD")
)

const (
	// size of the seed array. Each seed is a 64bits int (8 bytes)
	// totaling 16mb
	seedArraySize = 2 * 1024 * 1024

	numOfSeedsStripes = 512
)

type ExpandedPostingsCacheMetrics struct {
	CacheRequests       *prometheus.CounterVec
	CacheHits           *prometheus.CounterVec
	CacheEvicts         *prometheus.CounterVec
	CacheMiss           *prometheus.CounterVec
	NonCacheableQueries *prometheus.CounterVec
	LazyMatcherQueries  prometheus.Counter
}

func NewPostingCacheMetrics(r prometheus.Registerer) *ExpandedPostingsCacheMetrics {
	return &ExpandedPostingsCacheMetrics{
		CacheRequests: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_expanded_postings_cache_requests_total",
			Help: "Total number of requests to the cache.",
		}, []string{"cache"}),
		CacheHits: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_expanded_postings_cache_hits_total",
			Help: "Total number of hit requests to the cache.",
		}, []string{"cache"}),
		CacheMiss: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_expanded_postings_cache_miss_total",
			Help: "Total number of miss requests to the cache.",
		}, []string{"cache", "reason"}),
		CacheEvicts: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_expanded_postings_cache_evicts_total",
			Help: "Total number of evictions in the cache, excluding items that got evicted due to TTL.",
		}, []string{"cache", "reason"}),
		NonCacheableQueries: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_expanded_postings_non_cacheable_queries_total",
			Help: "Total number of non cacheable queries.",
		}, []string{"cache"}),
		LazyMatcherQueries: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_expanded_postings_lazy_matcher_queries_total",
			Help: "Total number of queries that used lazy matcher evaluation on cache miss.",
		}),
	}
}

type TSDBPostingsCacheConfig struct {
	Head   PostingsCacheConfig `yaml:"head" doc:"description=If enabled, ingesters will cache expanded postings for the head block. Only queries with an equal matcher for metric __name__ are cached."`
	Blocks PostingsCacheConfig `yaml:"blocks" doc:"description=If enabled, ingesters will cache expanded postings for the compacted blocks. The cache is shared between all blocks."`

	// LazyMatcherMaxCardinality configures the maximum label cardinality threshold for
	// deferring regex matchers on the head block. When a regex matcher targets a label with
	// more unique values than this threshold, the matcher is applied lazily during series
	// iteration instead of during postings lookup. This avoids expensive regex scans on
	// high-cardinality labels when the head postings cache misses. 0 disables this optimization.
	LazyMatcherMaxCardinality int `yaml:"lazy_matcher_max_cardinality"`

	// LazyMatcherSimpleCostRatio is the cardinality:postings ratio above which a
	// regex matcher with a simple per-call cost (prefix-only, single contains,
	// single suffix) is deferred to lazy iteration. Tuned empirically — see
	// regexCostClass for derivation. Defaults to 6 when unset.
	LazyMatcherSimpleCostRatio int `yaml:"lazy_matcher_simple_cost_ratio"`

	// LazyMatcherComplexCostRatio is the cardinality:postings ratio above which a
	// regex matcher with a complex per-call cost (multi-substring contains,
	// capture groups with literals, character classes) is deferred. Defaults to 2
	// when unset.
	LazyMatcherComplexCostRatio int `yaml:"lazy_matcher_complex_cost_ratio"`

	// The configurations below are used only for testing purpose
	PostingsForMatchers func(ctx context.Context, ix tsdb.IndexReader, ms ...*labels.Matcher) (index.Postings, error) `yaml:"-"`
	SeedSize            int                                                                                           `yaml:"-"`
	timeNow             func() time.Time                                                                              `yaml:"-"`
}

type PostingsCacheConfig struct {
	Enabled      bool          `yaml:"enabled"`
	MaxBytes     int64         `yaml:"max_bytes"`
	Ttl          time.Duration `yaml:"ttl"`
	FetchTimeout time.Duration `yaml:"fetch_timeout"`
}

func (cfg *TSDBPostingsCacheConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.Head.RegisterFlagsWithPrefix(prefix, "head", f)
	cfg.Blocks.RegisterFlagsWithPrefix(prefix, "block", f)
	f.IntVar(&cfg.LazyMatcherMaxCardinality, prefix+"expanded_postings_cache.head.lazy-matcher-max-cardinality", 0, "[EXPERIMENTAL] Maximum label cardinality for deferring regex matchers on the head block. When a regex matcher targets a label with more unique values than this threshold, it is applied lazily during iteration instead of postings lookup. 0 disables.")
	f.IntVar(&cfg.LazyMatcherSimpleCostRatio, prefix+"expanded_postings_cache.head.lazy-matcher-simple-cost-ratio", defaultSimpleCostRatio, "[EXPERIMENTAL] Cardinality:postings ratio above which a simple regex (prefix-only, single contains) is deferred to lazy iteration. Lower = more aggressive deferral. Calibrated empirically; defaults to 6.")
	f.IntVar(&cfg.LazyMatcherComplexCostRatio, prefix+"expanded_postings_cache.head.lazy-matcher-complex-cost-ratio", defaultComplexCostRatio, "[EXPERIMENTAL] Cardinality:postings ratio above which a complex regex (multi-substring, capture groups, character classes) is deferred. Lower = more aggressive deferral. Calibrated empirically; defaults to 2.")
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet
func (cfg *PostingsCacheConfig) RegisterFlagsWithPrefix(prefix, block string, f *flag.FlagSet) {
	f.Int64Var(&cfg.MaxBytes, prefix+"expanded_postings_cache."+block+".max-bytes", 10*1024*1024, "Max bytes for postings cache")
	f.DurationVar(&cfg.Ttl, prefix+"expanded_postings_cache."+block+".ttl", 10*time.Minute, "TTL for postings cache")
	f.DurationVar(&cfg.FetchTimeout, prefix+"expanded_postings_cache."+block+".fetch-timeout", 0, "Timeout for fetching postings from TSDB index when cache miss occurs. This prevents runaway queries from consuming resources when all callers have given up.")
	f.BoolVar(&cfg.Enabled, prefix+"expanded_postings_cache."+block+".enabled", false, "Whether the postings cache is enabled or not")
}

type ExpandedPostingsCacheFactory struct {
	seedByHash *seedByHash
	cfg        TSDBPostingsCacheConfig
}

func NewExpandedPostingsCacheFactory(cfg TSDBPostingsCacheConfig) *ExpandedPostingsCacheFactory {
	if cfg.Head.Enabled || cfg.Blocks.Enabled {
		if cfg.SeedSize == 0 {
			cfg.SeedSize = seedArraySize
		}
		logutil.WarnExperimentalUse("expanded postings cache")
		return &ExpandedPostingsCacheFactory{
			cfg:        cfg,
			seedByHash: newSeedByHash(cfg.SeedSize),
		}
	}

	return nil
}

func (f *ExpandedPostingsCacheFactory) NewExpandedPostingsCache(userId string, metrics *ExpandedPostingsCacheMetrics) ExpandedPostingsCache {
	return newBlocksPostingsForMatchersCache(userId, f.cfg, metrics, f.seedByHash)
}

type ExpandedPostingsCache interface {
	PostingsForMatchers(ctx context.Context, blockID ulid.ULID, ix tsdb.IndexReader, ms ...*labels.Matcher) (index.Postings, error)
	ExpireSeries(metric labels.Labels)
	PurgeExpiredItems()
	Clear()
	Size() int
}

type blocksPostingsForMatchersCache struct {
	userId string

	headCache               *lruCache[[]storage.SeriesRef]
	blocksCache             *lruCache[[]storage.SeriesRef]
	postingsForMatchersFunc func(ctx context.Context, ix tsdb.IndexReader, ms ...*labels.Matcher) (index.Postings, error)
	timeNow                 func() time.Time

	lazyMatcherCfg        lazyMatcherConfig
	labelCardinalityCache *expirable.LRU[string, int]

	metrics    *ExpandedPostingsCacheMetrics
	seedByHash *seedByHash
}

func (c *blocksPostingsForMatchersCache) Clear() {
	c.headCache.clear()
	c.blocksCache.clear()
}

func newBlocksPostingsForMatchersCache(userId string, cfg TSDBPostingsCacheConfig, metrics *ExpandedPostingsCacheMetrics, seedByHash *seedByHash) ExpandedPostingsCache {
	if cfg.PostingsForMatchers == nil {
		cfg.PostingsForMatchers = tsdb.PostingsForMatchers
	}

	if cfg.timeNow == nil {
		cfg.timeNow = time.Now
	}

	return &blocksPostingsForMatchersCache{
		headCache:               newLruCache[[]storage.SeriesRef](cfg.Head, "head", metrics, cfg.timeNow),
		blocksCache:             newLruCache[[]storage.SeriesRef](cfg.Blocks, "block", metrics, cfg.timeNow),
		postingsForMatchersFunc: cfg.PostingsForMatchers,
		timeNow:                 cfg.timeNow,
		lazyMatcherCfg: lazyMatcherConfig{
			MaxCardinality: cfg.LazyMatcherMaxCardinality,
			SimpleRatio:    cfg.LazyMatcherSimpleCostRatio,
			ComplexRatio:   cfg.LazyMatcherComplexCostRatio,
		},
		labelCardinalityCache: newLabelCardinalityCache(),
		metrics:               metrics,
		seedByHash:            seedByHash,
		userId:                userId,
	}
}

func (c *blocksPostingsForMatchersCache) ExpireSeries(metric labels.Labels) {
	metricName, err := extract.MetricNameFromLabels(metric)
	if err != nil {
		return
	}
	c.seedByHash.incrementSeed(c.userId, metricName)
}

func (c *blocksPostingsForMatchersCache) PurgeExpiredItems() {
	c.headCache.expire()
	c.blocksCache.expire()
}

func (c *blocksPostingsForMatchersCache) Size() int {
	return c.headCache.size() + c.blocksCache.size()
}

func (c *blocksPostingsForMatchersCache) PostingsForMatchers(ctx context.Context, blockID ulid.ULID, ix tsdb.IndexReader, ms ...*labels.Matcher) (index.Postings, error) {
	return c.fetchPostings(blockID, ix, ms...)(ctx)
}

func (c *blocksPostingsForMatchersCache) fetchPostings(blockID ulid.ULID, ix tsdb.IndexReader, ms ...*labels.Matcher) func(context.Context) (index.Postings, error) {
	var seed string
	cache := c.blocksCache

	// If is a head block, lets add the seed on the cache key so we can
	// invalidate the cache when new series are created for this metric name
	if isHeadBlock(blockID) {
		cache = c.headCache
		if cache.cfg.Enabled {
			metricName, ok := metricNameFromMatcher(ms)
			// Lets not cache head if we don;t find an equal matcher for the label __name__
			if !ok {
				c.metrics.NonCacheableQueries.WithLabelValues(cache.name).Inc()
				return func(ctx context.Context) (index.Postings, error) {
					return tsdb.PostingsForMatchers(ctx, ix, ms...)
				}
			}

			seed = c.getSeedForMetricName(metricName)
		}
	}

	// Let's bypass cache if not enabled
	if !cache.cfg.Enabled {
		return func(ctx context.Context) (index.Postings, error) {
			return tsdb.PostingsForMatchers(ctx, ix, ms...)
		}
	}

	c.metrics.CacheRequests.WithLabelValues(cache.name).Inc()

	fetch := func() ([]storage.SeriesRef, int64, error) {
		// Use a context with timeout instead of context.Background() to prevent runaway queries.
		// This promise is maybe shared across calls, so we can't use any single caller's context.
		// However, we need a timeout to prevent the fetch from running indefinitely when all
		// callers have given up (e.g., after their 1-minute query timeout).
		fetchCtx := context.Background()
		if cache.cfg.FetchTimeout > 0 {
			var cancel context.CancelFunc
			fetchCtx, cancel = context.WithTimeout(fetchCtx, cache.cfg.FetchTimeout)
			defer cancel()
		}

		// For head blocks, try to avoid expensive regex scans by splitting matchers:
		// resolve postings with selective matchers only, then filter by regex lazily.
		if isHeadBlock(blockID) && c.lazyMatcherCfg.MaxCardinality > 0 {
			selectMs, lazyMs := splitMatchersForHeadWithConfig(fetchCtx, ix, ms, c.lazyMatcherCfg, c.labelCardinalityCache)
			if len(lazyMs) > 0 {
				c.metrics.LazyMatcherQueries.Inc()
				return c.fetchWithLazyMatchers(fetchCtx, ix, selectMs, lazyMs)
			}
		}

		postings, err := c.postingsForMatchersFunc(fetchCtx, ix, ms...)

		if err == nil {
			ids, err := index.ExpandPostings(postings)
			return ids, int64(len(ids) * 8), err
		}

		return nil, 0, err
	}

	key := cacheKey(seed, blockID, ms...)
	promise, loaded := cache.getPromiseForKey(key, fetch)
	if loaded {
		c.metrics.CacheHits.WithLabelValues(cache.name).Inc()
	}

	return c.result(promise)
}

func (c *blocksPostingsForMatchersCache) result(ce *cacheEntryPromise[[]storage.SeriesRef]) func(ctx context.Context) (index.Postings, error) {
	return func(ctx context.Context) (index.Postings, error) {
		if call := ce.call.Load(); call != nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-call.done:
			}
			if call.err != nil {
				return nil, call.err
			}
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return index.NewListPostings(ce.v), nil
	}
}

// fetchWithLazyMatchers resolves postings using only the selective matchers, then
// filters the results by applying the lazy (regex) matchers per-series using
// Series. We call Series once per series ref to retrieve all labels, then
// evaluate each lazy matcher against the relevant label value. A per-matcher,
// per-value cache avoids running the same regex against the same string more
// than once, which is particularly effective when many series share label values.
func (c *blocksPostingsForMatchersCache) fetchWithLazyMatchers(ctx context.Context, ix tsdb.IndexReader, selectMs, lazyMs []*labels.Matcher) ([]storage.SeriesRef, int64, error) {
	postings, err := c.postingsForMatchersFunc(ctx, ix, selectMs...)
	if err != nil {
		return nil, 0, err
	}

	ids, err := index.ExpandPostings(postings)
	if err != nil {
		return nil, 0, err
	}

	// Per-matcher cache: label value -> match result
	caches := make([]map[string]bool, len(lazyMs))
	for i := range lazyMs {
		caches[i] = make(map[string]bool)
	}

	var builder labels.ScratchBuilder
	// reusedLabels is only used by the slicelabels build, where Overwrite reuses
	// its backing slice to avoid a per-series allocation. Other builds ignore it.
	var reusedLabels labels.Labels
	filtered := ids[:0]
	for cnt, id := range ids {
		if cnt%128 == 0 && ctx.Err() != nil {
			return nil, 0, ctx.Err()
		}

		if err := ix.Series(id, &builder, nil); err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return nil, 0, err
		}
		lbls := seriesLabelsForLazyMatch(&builder, &reusedLabels)

		matches := true
		for i, m := range lazyMs {
			val := lbls.Get(m.Name)

			if result, ok := caches[i][val]; ok {
				if !result {
					matches = false
					break
				}
				continue
			}

			result := m.Matches(val)
			caches[i][val] = result
			if !result {
				matches = false
				break
			}
		}
		if matches {
			filtered = append(filtered, id)
		}
	}

	return filtered, int64(len(filtered) * 8), nil
}

func (c *blocksPostingsForMatchersCache) getSeedForMetricName(metricName string) string {
	return c.seedByHash.getSeed(c.userId, metricName)
}

func cacheKey(seed string, blockID ulid.ULID, ms ...*labels.Matcher) string {
	slices.SortFunc(ms, func(i, j *labels.Matcher) int {
		if i.Type != j.Type {
			return int(i.Type - j.Type)
		}
		if i.Name != j.Name {
			return strings.Compare(i.Name, j.Name)
		}
		if i.Value != j.Value {
			return strings.Compare(i.Value, j.Value)
		}
		return 0
	})

	const (
		typeLen = 2
		sepLen  = 1
	)

	size := len(seed) + len(blockID.String()) + 2*sepLen
	for _, m := range ms {
		size += len(m.Name) + len(m.Value) + typeLen + sepLen
	}
	sb := strings.Builder{}
	sb.Grow(size)
	sb.WriteString(seed)
	sb.WriteByte('|')
	sb.WriteString(blockID.String())
	sb.WriteByte('|')
	for _, m := range ms {
		sb.WriteString(m.Name)
		sb.WriteString(m.Type.String())
		sb.WriteString(m.Value)
		sb.WriteByte('|')
	}
	key := sb.String()
	return key
}

func isHeadBlock(blockID ulid.ULID) bool {
	return blockID == rangeHeadULID || blockID == headULID
}

func metricNameFromMatcher(ms []*labels.Matcher) (string, bool) {
	for _, m := range ms {
		if m.Name == labels.MetricName && m.Type == labels.MatchEqual {
			return m.Value, true
		}
	}

	return "", false
}

type seedByHash struct {
	strippedLock []sync.RWMutex
	seedByHash   []int
}

func newSeedByHash(size int) *seedByHash {
	return &seedByHash{
		seedByHash:   make([]int, size),
		strippedLock: make([]sync.RWMutex, numOfSeedsStripes),
	}
}

func (s *seedByHash) getSeed(userId string, v string) string {
	h := memHashString(userId, v)
	i := h % uint64(len(s.seedByHash))
	l := i % uint64(len(s.strippedLock))
	s.strippedLock[l].RLock()
	defer s.strippedLock[l].RUnlock()
	return strconv.Itoa(s.seedByHash[i])
}

func (s *seedByHash) incrementSeed(userId string, v string) {
	h := memHashString(userId, v)
	i := h % uint64(len(s.seedByHash))
	l := i % uint64(len(s.strippedLock))
	s.strippedLock[l].Lock()
	defer s.strippedLock[l].Unlock()
	s.seedByHash[i]++
}

// inflight holds what concurrent callers wait on while a value is being fetched.
// It is dropped once the fetch succeeds, so completed entries carry no channel.
type inflight struct {
	done chan struct{}
	err  error
}

// cacheEntryPromise is the single object kept per cached key. It is linked in an
// intrusive LRU list, so no separate list element or boxed key is allocated.
type cacheEntryPromise[V any] struct {
	key        string
	prev, next *cacheEntryPromise[V]
	v          V
	// call is non-nil while the fetch runs, and stays set (with err) for a failed fetch.
	call      atomic.Pointer[inflight]
	tsNanos   int64
	sizeBytes int64
}

// lruCache is a byte-bounded LRU with TTL. All map and list changes happen under
// mu, and an entry is in cachedValues if and only if it is linked in the list.
type lruCache[V any] struct {
	cfg     PostingsCacheConfig
	timeNow func() time.Time
	name    string
	metrics ExpandedPostingsCacheMetrics

	mu           sync.Mutex
	cachedValues map[string]*cacheEntryPromise[V]
	front, back  *cacheEntryPromise[V] // least and most recently used
	cachedBytes  int64
}

func newLruCache[V any](cfg PostingsCacheConfig, name string, metrics *ExpandedPostingsCacheMetrics, timeNow func() time.Time) *lruCache[V] {
	return &lruCache[V]{
		cachedValues: make(map[string]*cacheEntryPromise[V]),
		cfg:          cfg,
		timeNow:      timeNow,
		name:         name,
		metrics:      *metrics,
	}
}

func (c *lruCache[V]) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cachedValues = make(map[string]*cacheEntryPromise[V])
	c.front, c.back = nil, nil
	c.cachedBytes = 0
}

func (c *lruCache[V]) expire() {
	if c.cfg.Ttl <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evictLocked()
}

func (c *lruCache[V]) size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.cachedValues)
}

func (c *lruCache[V]) contains(k string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.cachedValues[k]
	return ok
}

// getPromiseForKey returns the entry for k, starting a fetch on a miss or after expiry.
// It does not wait: callers wait through the entry's inflight call, honoring their context.
func (c *lruCache[V]) getPromiseForKey(k string, fetch func() (V, int64, error)) (*cacheEntryPromise[V], bool) {
	if !c.cfg.Enabled {
		e := &cacheEntryPromise[V]{key: k}
		var err error
		e.v, _, err = fetch()
		if err != nil {
			e.call.Store(&inflight{done: closedChan, err: err})
		}
		return e, false
	}

	c.mu.Lock()
	reason := "miss"
	if e, ok := c.cachedValues[k]; ok {
		if e.call.Load() != nil || !c.isExpiredLocked(e, c.timeNow()) {
			c.moveToBackLocked(e)
			c.mu.Unlock()
			return e, true
		}
		reason = "expired"
		c.removeLocked(e)
	}
	e := &cacheEntryPromise[V]{key: k}
	call := &inflight{done: make(chan struct{})}
	e.call.Store(call)
	c.cachedValues[k] = e
	c.pushBackLocked(e)
	c.mu.Unlock()

	c.metrics.CacheMiss.WithLabelValues(c.name, reason).Inc()
	v, size, err := c.runFetch(k, e, call, fetch)

	c.mu.Lock()
	if err != nil {
		// Never cache failures: drop the entry so the next caller fetches again.
		if c.cachedValues[k] == e {
			c.removeLocked(e)
		}
		c.mu.Unlock()
		call.err = err
		close(call.done)
		return e, false
	}
	e.v = v
	e.tsNanos = c.timeNow().UnixNano()
	e.sizeBytes = size + int64(len(k))
	if c.cachedValues[k] == e {
		if c.cfg.Ttl <= 0 {
			c.removeLocked(e)
		} else {
			c.cachedBytes += e.sizeBytes
		}
	}
	e.call.Store(nil)
	// Enforce the byte budget and sweep expired entries on every completed fetch,
	// including the refresh of an expired key.
	c.evictLocked()
	c.mu.Unlock()
	close(call.done)
	return e, false
}

// errFetchPanicked is returned to callers waiting on a fetch that panicked.
var errFetchPanicked = errors.New("expanded postings fetch panicked")

// runFetch calls fetch. If it panics, the entry is dropped and its waiters are woken with
// errFetchPanicked before the panic continues, so the key is not left in flight forever.
func (c *lruCache[V]) runFetch(k string, e *cacheEntryPromise[V], call *inflight, fetch func() (V, int64, error)) (v V, size int64, err error) {
	completed := false
	defer func() {
		if completed {
			return
		}
		c.mu.Lock()
		if c.cachedValues[k] == e {
			c.removeLocked(e)
		}
		c.mu.Unlock()
		call.err = errFetchPanicked
		close(call.done)
	}()
	v, size, err = fetch()
	completed = true
	return v, size, err
}

func (c *lruCache[V]) isExpiredLocked(e *cacheEntryPromise[V], now time.Time) bool {
	return now.UnixNano()-e.tsNanos >= c.cfg.Ttl.Nanoseconds()
}

// evictLocked drops least recently used entries while over budget, then expired ones.
// Entries whose fetch is still running are never expired.
func (c *lruCache[V]) evictLocked() {
	now := c.timeNow()
	for c.front != nil {
		reason := ""
		switch {
		case c.cachedBytes > c.cfg.MaxBytes:
			reason = "full"
		case c.front.call.Load() == nil && c.isExpiredLocked(c.front, now):
			reason = "expired"
		default:
			return
		}
		c.metrics.CacheEvicts.WithLabelValues(c.name, reason).Inc()
		c.removeLocked(c.front)
	}
}

func (c *lruCache[V]) pushBackLocked(e *cacheEntryPromise[V]) {
	e.prev, e.next = c.back, nil
	if c.back != nil {
		c.back.next = e
	} else {
		c.front = e
	}
	c.back = e
}

func (c *lruCache[V]) unlinkLocked(e *cacheEntryPromise[V]) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		c.front = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		c.back = e.prev
	}
	e.prev, e.next = nil, nil
}

func (c *lruCache[V]) moveToBackLocked(e *cacheEntryPromise[V]) {
	if c.back == e {
		return
	}
	c.unlinkLocked(e)
	c.pushBackLocked(e)
}

// removeLocked unlinks e and deletes it from the map. The bytes of an entry whose
// fetch is still running are not counted yet, so they are not subtracted.
func (c *lruCache[V]) removeLocked(e *cacheEntryPromise[V]) {
	c.unlinkLocked(e)
	delete(c.cachedValues, e.key)
	if e.call.Load() == nil {
		c.cachedBytes -= e.sizeBytes
	}
}

// closedChan is shared by results that are already complete when returned.
var closedChan = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

func memHashString(userId, v string) uint64 {
	h := fnv1a.HashString64(userId)
	return fnv1a.AddString64(h, v)
}
