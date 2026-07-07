package tsdb

import (
	"container/list"
	"context"
	"errors"
	"flag"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/segmentio/fasthash/fnv1a"

	"github.com/cortexproject/cortex/pkg/util/extract"
	logutil "github.com/cortexproject/cortex/pkg/util/log"
)

var (
	rangeHeadULID = ulid.MustParse("0000000000XXXXXXXRANGEHEAD")
	headULID      = ulid.MustParse("0000000000XXXXXXXXXXXXHEAD")
)

const (
	// size of the label-count array. Each count is a uint32 (4 bytes) totaling 16MB.
	labelCounterArraySize = 4_000_000
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
	Head   PostingsCacheConfig `yaml:"head" doc:"description=If enabled, ingesters will cache expanded postings for the head block. Only queries with with an equal matcher for metric __name__ are cached."`
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

	// LabelCounterSize is the number of counts stored in the label counter used
	// for head cache invalidation. Note one label counter is shared by all tenants.
	// defaults to 4_000_000 when unset.
	LabelCounterSize uint `yaml:"label_counter_size"`

	// The configurations below are used only for testing purpose
	PostingsForMatchers func(ctx context.Context, ix tsdb.IndexReader, ms ...*labels.Matcher) (index.Postings, error) `yaml:"-"`
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
	f.UintVar(&cfg.LabelCounterSize, prefix+"expanded_postings_cache.head.label-counter-size", labelCounterArraySize, "The number of counts stored in the label counter used for head cache invalidation. Note one label counter is shared by all tenants. 0 sets to the default of 4_000_000.")
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet
func (cfg *PostingsCacheConfig) RegisterFlagsWithPrefix(prefix, block string, f *flag.FlagSet) {
	f.Int64Var(&cfg.MaxBytes, prefix+"expanded_postings_cache."+block+".max-bytes", 10*1024*1024, "Max bytes for postings cache")
	f.DurationVar(&cfg.Ttl, prefix+"expanded_postings_cache."+block+".ttl", 10*time.Minute, "TTL for postings cache")
	f.DurationVar(&cfg.FetchTimeout, prefix+"expanded_postings_cache."+block+".fetch-timeout", 0, "Timeout for fetching postings from TSDB index when cache miss occurs. This prevents runaway queries from consuming resources when all callers have given up.")
	f.BoolVar(&cfg.Enabled, prefix+"expanded_postings_cache."+block+".enabled", false, "Whether the postings cache is enabled or not")
}

type ExpandedPostingsCacheFactory struct {
	labelCounter *labelCounter
	cfg          TSDBPostingsCacheConfig
}

func NewExpandedPostingsCacheFactory(cfg TSDBPostingsCacheConfig) *ExpandedPostingsCacheFactory {
	if cfg.Head.Enabled || cfg.Blocks.Enabled {
		if cfg.LabelCounterSize == 0 {
			cfg.LabelCounterSize = labelCounterArraySize
		}
		logutil.WarnExperimentalUse("expanded postings cache")
		return &ExpandedPostingsCacheFactory{
			cfg:          cfg,
			labelCounter: newLabelCounter(cfg.LabelCounterSize),
		}
	}

	return nil
}

func (f *ExpandedPostingsCacheFactory) NewExpandedPostingsCache(userId string, metrics *ExpandedPostingsCacheMetrics) ExpandedPostingsCache {
	return newBlocksPostingsForMatchersCache(userId, f.cfg, metrics, f.labelCounter)
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

	metrics      *ExpandedPostingsCacheMetrics
	labelCounter *labelCounter
}

func (c *blocksPostingsForMatchersCache) Clear() {
	c.headCache.clear()
	c.blocksCache.clear()
}

func newBlocksPostingsForMatchersCache(userId string, cfg TSDBPostingsCacheConfig, metrics *ExpandedPostingsCacheMetrics, labelCounter *labelCounter) ExpandedPostingsCache {
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
		labelCounter:          labelCounter,
		userId:                userId,
	}
}

func (c *blocksPostingsForMatchersCache) ExpireSeries(metric labels.Labels) {
	metricName, err := extract.MetricNameFromLabels(metric)
	if err != nil {
		return
	}

	metric.Range(func(label labels.Label) {
		if label.Name == model.MetricNameLabel {
			c.labelCounter.increment(c.userId, label.Value)
		} else {
			c.labelCounter.increment(c.userId, metricName, label.Name)
		}
	})
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
	cache := c.blocksCache
	isHead := isHeadBlock(blockID)
	var metricName string

	// If is a head block, we snapshot the per-label write counts so we can invalidate
	// the cache when new series are created for this metric name.
	if isHead {
		cache = c.headCache
		if cache.cfg.Enabled {
			var ok bool
			metricName, ok = metricNameFromMatcher(ms)
			// Lets not cache head if we don;t find an equal matcher for the label __name__
			if !ok {
				c.metrics.NonCacheableQueries.WithLabelValues(cache.name).Inc()
				return func(ctx context.Context) (index.Postings, error) {
					return tsdb.PostingsForMatchers(ctx, ix, ms...)
				}
			}
		}
	}

	// Let's bypass cache if not enabled
	if !cache.cfg.Enabled {
		return func(ctx context.Context) (index.Postings, error) {
			return tsdb.PostingsForMatchers(ctx, ix, ms...)
		}
	}

	c.metrics.CacheRequests.WithLabelValues(cache.name).Inc()

	fetch := func() ([]storage.SeriesRef, int64, func() bool, error) {
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

		// Count-based invalidation only applies to the head cache: series are created
		// there and bump the per-label counts. Block entries are immutable, so they
		// get a nil validator and rely solely on TTL expiry.
		var stillValid func() bool
		var snapshotSize int64
		if isHead {
			stillValid, snapshotSize = c.snapshotCounts(metricName, ms...)
		}

		// For head blocks, try to avoid expensive regex scans by splitting matchers:
		// resolve postings with selective matchers only, then filter by regex lazily.
		if isHead && c.lazyMatcherCfg.MaxCardinality > 0 {
			selectMs, lazyMs := splitMatchersForHeadWithConfig(fetchCtx, ix, ms, c.lazyMatcherCfg, c.labelCardinalityCache)
			if len(lazyMs) > 0 {
				c.metrics.LazyMatcherQueries.Inc()
				series, size, err := c.fetchWithLazyMatchers(fetchCtx, ix, selectMs, lazyMs)
				return series, size + snapshotSize, stillValid, err
			}
		}

		postings, err := c.postingsForMatchersFunc(fetchCtx, ix, ms...)

		if err == nil {
			ids, err := index.ExpandPostings(postings)
			return ids, int64(len(ids)*8) + snapshotSize, stillValid, err
		}

		return nil, 0, stillValid, err
	}

	key := cacheKey(blockID, ms...)
	promise, loaded := cache.getPromiseForKey(key, fetch)
	if loaded {
		c.metrics.CacheHits.WithLabelValues(cache.name).Inc()
	}

	return c.result(promise)
}

// snapshotCounts records the current per-label write counts for the matchers so the
// cached entry can later be validated. The slot for each matcher MUST be keyed
// identically to ExpireSeries' increment: __name__ by its value, every other label by
// (metricName, labelName). A mismatch would read a slot that never moves and keep stale
// entries alive forever.
//
// The returned stillValid closure reports whether the entry is still complete: it holds
// as long as ANY tracked count is unchanged, because a series matching all matchers bumps
// every tracked slot, so all-counts-changed is the only proof of a possibly-missed series.
//
// Empty-matching matchers (e.g. foo!="x", foo=~".*") match series that lack the label, and
// such a series bumps __name__ but not (metric, foo). Their count could stay frozen while a
// matching series was created, so they cannot vouch for the entry and are skipped. The
// __name__ equal matcher never matches "", so at least one snapshot always remains.
func (c *blocksPostingsForMatchersCache) snapshotCounts(metricName string, ms ...*labels.Matcher) (func() bool, int64) {
	snapshots := make([]countSnapshot, 0, len(ms))
	for _, matcher := range ms {
		// Matchers that can match an absent label cannot vouch for the entry.
		if matcher.Matches("") {
			continue
		}

		var slot uint32
		if matcher.Name == model.MetricNameLabel {
			slot = c.labelCounter.slotIndex(c.userId, matcher.Value)
		} else {
			slot = c.labelCounter.slotIndex(c.userId, metricName, matcher.Name)
		}
		snapshots = append(snapshots, countSnapshot{
			index: slot,
			count: c.labelCounter.readSlot(slot),
		})
	}
	stillValid := func() bool {
		for _, snapshot := range snapshots {
			newCount := c.labelCounter.readSlot(snapshot.index)
			if newCount == snapshot.count {
				return true
			}
		}

		return false
	}

	// countSnapshot is two uint32s (8 bytes total)
	return stillValid, int64(len(snapshots) * 8)
}

func (c *blocksPostingsForMatchersCache) result(ce *cacheEntryPromise[[]storage.SeriesRef]) func(ctx context.Context) (index.Postings, error) {
	return func(ctx context.Context) (index.Postings, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ce.done:
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return index.NewListPostings(ce.v), ce.err
		}
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

func cacheKey(blockID ulid.ULID, ms ...*labels.Matcher) string {
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

	size := len(blockID.String()) + sepLen
	for _, m := range ms {
		size += len(m.Name) + len(m.Value) + typeLen + sepLen
	}
	sb := strings.Builder{}
	sb.Grow(size)
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
		if m.Name == model.MetricNameLabel && m.Type == labels.MatchEqual {
			return m.Value, true
		}
	}

	return "", false
}

// labelCounter tracks, per (tenant, label) hash slot, how many times a series carrying that
// label has been created or deleted. Cache entries snapshot these counts to detect when a
// matching series may have been added since the entry was stored. Collisions are safe: two
// distinct labels sharing a slot only cause extra (never missed) invalidations.
type labelCounter struct {
	counts []atomic.Uint32
}

func newLabelCounter(size uint) *labelCounter {
	return &labelCounter{
		counts: make([]atomic.Uint32, size),
	}
}

func (s *labelCounter) slotIndex(userId string, dimensions ...string) uint32 {
	hash := memHashStrings(userId, dimensions...)
	return hash % uint32(len(s.counts))
}

func (s *labelCounter) readSlot(index uint32) uint32 {
	return s.counts[index].Load()
}

func (s *labelCounter) increment(userId string, dimensions ...string) {
	i := s.slotIndex(userId, dimensions...)
	s.counts[i].Add(1)
}

type lruCache[V any] struct {
	cfg          PostingsCacheConfig
	cachedValues *sync.Map
	timeNow      func() time.Time
	name         string
	metrics      ExpandedPostingsCacheMetrics

	// Fields from here should be locked
	cachedMtx   sync.RWMutex
	cached      *list.List
	cachedBytes int64
}

func newLruCache[V any](cfg PostingsCacheConfig, name string, metrics *ExpandedPostingsCacheMetrics, timeNow func() time.Time) *lruCache[V] {
	return &lruCache[V]{
		cachedValues: new(sync.Map),
		cached:       list.New(),
		cfg:          cfg,
		timeNow:      timeNow,
		name:         name,
		metrics:      *metrics,
	}
}

func (c *lruCache[V]) clear() {
	c.cachedMtx.Lock()
	defer c.cachedMtx.Unlock()
	c.cached = list.New()
	c.cachedBytes = 0
	c.cachedValues = new(sync.Map)
}

func (c *lruCache[V]) expire() {
	if c.cfg.Ttl <= 0 {
		return
	}
	c.cachedMtx.RLock()
	if _, r := c.shouldEvictHead(); !r {
		c.cachedMtx.RUnlock()
		return
	}
	c.cachedMtx.RUnlock()
	c.cachedMtx.Lock()
	defer c.cachedMtx.Unlock()
	for reason, r := c.shouldEvictHead(); r; reason, r = c.shouldEvictHead() {
		c.metrics.CacheEvicts.WithLabelValues(c.name, reason).Inc()
		c.evictHead()
	}
}

func (c *lruCache[V]) size() int {
	c.cachedMtx.RLock()
	defer c.cachedMtx.RUnlock()
	return c.cached.Len()
}

func (c *lruCache[V]) getPromiseForKey(k string, fetch func() (V, int64, func() bool, error)) (*cacheEntryPromise[V], bool) {
	r := &cacheEntryPromise[V]{
		done: make(chan struct{}),
	}
	defer close(r.done)

	if !c.cfg.Enabled {
		r.v, _, _, r.err = fetch()
		return r, false
	}

	loaded, ok := c.cachedValues.LoadOrStore(k, r)

	if !ok {
		c.metrics.CacheMiss.WithLabelValues(c.name, "miss").Inc()
		r.v, r.sizeBytes, r.stillValid, r.err = fetch()
		r.sizeBytes += int64(len(k))
		r.ts = c.timeNow()
		c.created(k, r.sizeBytes)
		c.expire()
	}

	if ok {
		entry := loaded.(*cacheEntryPromise[V])
		// If the promise is already in the cache, lets wait it to fetch the data.
		<-entry.done

		// LRU: move to back on access
		c.cachedMtx.Lock()
		if entry.elem != nil {
			c.cached.MoveToBack(entry.elem)
		}
		c.cachedMtx.Unlock()

		// A nil stillValid means the entry has no count-based invalidation (blocks
		// cache, or callers that don't snapshot counts); only time expiry applies.
		invalidated := entry.stillValid != nil && !entry.stillValid()

		// If is cached but is expired or invalidated, lets try to replace the cache value.
		if (invalidated || entry.isExpired(c.cfg.Ttl, c.timeNow())) && c.cachedValues.CompareAndSwap(k, loaded, r) {
			if invalidated {
				c.metrics.CacheMiss.WithLabelValues(c.name, "invalidated").Inc()
			} else {
				c.metrics.CacheMiss.WithLabelValues(c.name, "expired").Inc()
			}
			r.v, r.sizeBytes, r.stillValid, r.err = fetch()
			r.sizeBytes += int64(len(k))
			c.updateSize(loaded.(*cacheEntryPromise[V]).sizeBytes, r.sizeBytes)
			r.ts = c.timeNow()
			// Replace the list element: remove old, push new to back
			c.cachedMtx.Lock()
			if oldElem := loaded.(*cacheEntryPromise[V]).elem; oldElem != nil {
				c.cached.Remove(oldElem)
			}
			r.elem = c.cached.PushBack(k)
			c.cachedMtx.Unlock()
			loaded = r
			ok = false
		}
	}

	return loaded.(*cacheEntryPromise[V]), ok
}

func (c *lruCache[V]) contains(k string) bool {
	_, ok := c.cachedValues.Load(k)
	return ok
}

func (c *lruCache[V]) shouldEvictHead() (string, bool) {
	h := c.cached.Front()
	if h == nil {
		return "", false
	}

	if c.cachedBytes > c.cfg.MaxBytes {
		return "full", true
	}

	key := h.Value.(string)

	if l, ok := c.cachedValues.Load(key); ok {
		if l.(*cacheEntryPromise[V]).isExpired(c.cfg.Ttl, c.timeNow()) {
			return "expired", true
		}
	}

	return "", false
}

func (c *lruCache[V]) evictHead() {
	front := c.cached.Front()
	c.cached.Remove(front)
	oldestKey := front.Value.(string)
	if oldest, loaded := c.cachedValues.LoadAndDelete(oldestKey); loaded {
		c.cachedBytes -= oldest.(*cacheEntryPromise[V]).sizeBytes
	}
}

func (c *lruCache[V]) created(key string, sizeBytes int64) {
	if c.cfg.Ttl <= 0 {
		c.cachedValues.Delete(key)
		return
	}
	c.cachedMtx.Lock()
	defer c.cachedMtx.Unlock()
	elem := c.cached.PushBack(key)
	// Store the element reference in the promise for O(1) LRU access
	if p, ok := c.cachedValues.Load(key); ok {
		p.(*cacheEntryPromise[V]).elem = elem
	}
	c.cachedBytes += sizeBytes
}

func (c *lruCache[V]) updateSize(oldSize, newSizeBytes int64) {
	if oldSize == newSizeBytes {
		return
	}

	c.cachedMtx.Lock()
	defer c.cachedMtx.Unlock()
	c.cachedBytes += newSizeBytes - oldSize
}

type cacheEntryPromise[V any] struct {
	ts        time.Time
	sizeBytes int64
	elem      *list.Element

	stillValid func() bool

	done chan struct{}
	v    V
	err  error
}

func (ce *cacheEntryPromise[V]) isExpired(ttl time.Duration, now time.Time) bool {
	ts := ce.ts
	r := now.Sub(ts)
	return r >= ttl
}

func memHashStrings(userId string, dimensions ...string) uint32 {
	h := fnv1a.HashString32(userId)
	for _, d := range dimensions {
		h = fnv1a.AddString32(h, d)
	}
	return h
}

type countSnapshot struct {
	index uint32 // slot in labelCounter
	count uint32 // value at store time
}
