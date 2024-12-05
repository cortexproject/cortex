package tsdb

import (
	"container/list"
	"context"
	"flag"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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
	// size of the seed array. Each seed is a 64bits int (8 bytes)
	// totaling 16mb
	seedArraySize = 2 * 1024 * 1024

	numOfSeedsStripes = 512
)

type ExpandedPostingsCacheMetrics struct {
	CacheRequests       *prometheus.CounterVec
	CacheHits           *prometheus.CounterVec
	CacheEvicts         *prometheus.CounterVec
	NonCacheableQueries *prometheus.CounterVec
}

func NewPostingCacheMetrics(r prometheus.Registerer) *ExpandedPostingsCacheMetrics {
	return &ExpandedPostingsCacheMetrics{
		CacheRequests: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_expanded_postings_cache_requests",
			Help: "Total number of requests to the cache.",
		}, []string{"cache"}),
		CacheHits: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_expanded_postings_cache_hits",
			Help: "Total number of hit requests to the cache.",
		}, []string{"cache"}),
		CacheEvicts: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_expanded_postings_cache_evicts",
			Help: "Total number of evictions in the cache, excluding items that got evicted due to TTL.",
		}, []string{"cache", "reason"}),
		NonCacheableQueries: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_expanded_postings_non_cacheable_queries",
			Help: "Total number of non cacheable queries.",
		}, []string{"cache"}),
	}
}

type TSDBPostingsCacheConfig struct {
	Head   PostingsCacheConfig `yaml:"head" doc:"description=If enabled, ingesters will cache expanded postings for the head block. Only queries with with an equal matcher for metric __name__ are cached."`
	Blocks PostingsCacheConfig `yaml:"blocks" doc:"description=If enabled, ingesters will cache expanded postings for the compacted blocks. The cache is shared between all blocks."`

	// The configurations below are used only for testing purpose
	PostingsForMatchers func(ctx context.Context, ix tsdb.IndexReader, ms ...*labels.Matcher) (index.Postings, error) `yaml:"-"`
	SeedSize            int                                                                                           `yaml:"-"`
	timeNow             func() time.Time                                                                              `yaml:"-"`
}

type PostingsCacheConfig struct {
	Enabled  bool          `yaml:"enabled"`
	MaxBytes int64         `yaml:"max_bytes"`
	Ttl      time.Duration `yaml:"ttl"`
}

func (cfg *TSDBPostingsCacheConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.Head.RegisterFlagsWithPrefix(prefix, "head", f)
	cfg.Blocks.RegisterFlagsWithPrefix(prefix, "block", f)
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet
func (cfg *PostingsCacheConfig) RegisterFlagsWithPrefix(prefix, block string, f *flag.FlagSet) {
	f.Int64Var(&cfg.MaxBytes, prefix+"expanded_postings_cache."+block+".max-bytes", 10*1024*1024, "Max bytes for postings cache")
	f.DurationVar(&cfg.Ttl, prefix+"expanded_postings_cache."+block+".ttl", 10*time.Minute, "TTL for postings cache")
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
}

type blocksPostingsForMatchersCache struct {
	userId string

	headCache               *fifoCache[[]storage.SeriesRef]
	blocksCache             *fifoCache[[]storage.SeriesRef]
	postingsForMatchersFunc func(ctx context.Context, ix tsdb.IndexReader, ms ...*labels.Matcher) (index.Postings, error)
	timeNow                 func() time.Time

	metrics    *ExpandedPostingsCacheMetrics
	seedByHash *seedByHash
}

func newBlocksPostingsForMatchersCache(userId string, cfg TSDBPostingsCacheConfig, metrics *ExpandedPostingsCacheMetrics, seedByHash *seedByHash) ExpandedPostingsCache {
	if cfg.PostingsForMatchers == nil {
		cfg.PostingsForMatchers = tsdb.PostingsForMatchers
	}

	if cfg.timeNow == nil {
		cfg.timeNow = time.Now
	}

	return &blocksPostingsForMatchersCache{
		headCache:               newFifoCache[[]storage.SeriesRef](cfg.Head, "head", metrics, cfg.timeNow),
		blocksCache:             newFifoCache[[]storage.SeriesRef](cfg.Blocks, "block", metrics, cfg.timeNow),
		postingsForMatchersFunc: cfg.PostingsForMatchers,
		timeNow:                 cfg.timeNow,
		metrics:                 metrics,
		seedByHash:              seedByHash,
		userId:                  userId,
	}
}

func (c *blocksPostingsForMatchersCache) ExpireSeries(metric labels.Labels) {
	metricName, err := extract.MetricNameFromLabels(metric)
	if err != nil {
		return
	}
	c.seedByHash.incrementSeed(c.userId, metricName)
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
		// Use context.Background() as this promise is maybe shared across calls
		postings, err := c.postingsForMatchersFunc(context.Background(), ix, ms...)

		if err == nil {
			ids, err := index.ExpandPostings(postings)
			return ids, int64(len(ids) * 8), err
		}

		return nil, 0, err
	}

	key := c.cacheKey(seed, blockID, ms...)
	promise, loaded := cache.getPromiseForKey(key, fetch)
	if loaded {
		c.metrics.CacheHits.WithLabelValues(cache.name).Inc()
	}

	return c.result(promise)
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

func (c *blocksPostingsForMatchersCache) getSeedForMetricName(metricName string) string {
	return c.seedByHash.getSeed(c.userId, metricName)
}

func (c *blocksPostingsForMatchersCache) cacheKey(seed string, blockID ulid.ULID, ms ...*labels.Matcher) string {
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

	var size int
	for _, m := range ms {
		size += len(seed) + len(blockID.String()) + len(m.Name) + len(m.Value) + typeLen + 2*sepLen
	}
	sb := strings.Builder{}
	sb.Grow(size)
	sb.WriteString(seed)
	sb.WriteByte('|')
	sb.WriteString(blockID.String())
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

type fifoCache[V any] struct {
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

func newFifoCache[V any](cfg PostingsCacheConfig, name string, metrics *ExpandedPostingsCacheMetrics, timeNow func() time.Time) *fifoCache[V] {
	return &fifoCache[V]{
		cachedValues: new(sync.Map),
		cached:       list.New(),
		cfg:          cfg,
		timeNow:      timeNow,
		name:         name,
		metrics:      *metrics,
	}
}

func (c *fifoCache[V]) expire() {
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

func (c *fifoCache[V]) getPromiseForKey(k string, fetch func() (V, int64, error)) (*cacheEntryPromise[V], bool) {
	r := &cacheEntryPromise[V]{
		done: make(chan struct{}),
	}
	defer close(r.done)

	if !c.cfg.Enabled {
		r.v, _, r.err = fetch()
		return r, false
	}

	loaded, ok := c.cachedValues.LoadOrStore(k, r)

	if !ok {
		r.v, r.sizeBytes, r.err = fetch()
		r.sizeBytes += int64(len(k))
		r.ts = c.timeNow()
		c.created(k, r.sizeBytes)
		c.expire()
	}

	if ok {
		// If the promise is already in the cache, lets wait it to fetch the data.
		<-loaded.(*cacheEntryPromise[V]).done

		// If is cached but is expired, lets try to replace the cache value.
		if loaded.(*cacheEntryPromise[V]).isExpired(c.cfg.Ttl, c.timeNow()) && c.cachedValues.CompareAndSwap(k, loaded, r) {
			c.metrics.CacheEvicts.WithLabelValues(c.name, "expired").Inc()
			r.v, r.sizeBytes, r.err = fetch()
			r.sizeBytes += int64(len(k))
			c.updateSize(loaded.(*cacheEntryPromise[V]).sizeBytes, r.sizeBytes)
			loaded = r
			r.ts = c.timeNow()
			ok = false
		}
	}

	return loaded.(*cacheEntryPromise[V]), ok
}

func (c *fifoCache[V]) contains(k string) bool {
	_, ok := c.cachedValues.Load(k)
	return ok
}

func (c *fifoCache[V]) shouldEvictHead() (string, bool) {
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

func (c *fifoCache[V]) evictHead() {
	front := c.cached.Front()
	c.cached.Remove(front)
	oldestKey := front.Value.(string)
	if oldest, loaded := c.cachedValues.LoadAndDelete(oldestKey); loaded {
		c.cachedBytes -= oldest.(*cacheEntryPromise[V]).sizeBytes
	}
}

func (c *fifoCache[V]) created(key string, sizeBytes int64) {
	if c.cfg.Ttl <= 0 {
		c.cachedValues.Delete(key)
		return
	}
	c.cachedMtx.Lock()
	defer c.cachedMtx.Unlock()
	c.cached.PushBack(key)
	c.cachedBytes += sizeBytes
}

func (c *fifoCache[V]) updateSize(oldSize, newSizeBytes int64) {
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

	done chan struct{}
	v    V
	err  error
}

func (ce *cacheEntryPromise[V]) isExpired(ttl time.Duration, now time.Time) bool {
	ts := ce.ts
	r := now.Sub(ts)
	return r >= ttl
}

func memHashString(userId, v string) uint64 {
	h := fnv1a.HashString64(userId)
	return fnv1a.AddString64(h, v)
}
