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

	"github.com/cespare/xxhash/v2"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/cortexproject/cortex/pkg/util/extract"
)

var (
	rangeHeadULID = ulid.MustParse("0000000000XXXXXXXRANGEHEAD")
	headULID      = ulid.MustParse("0000000000XXXXXXXXXXXXHEAD")
)

const (
	// size of the seed array. Each seed is a 64bits int (8 bytes)
	// totaling 8mb
	seedArraySize = 1024 * 1024

	numOfSeedsStripes = 512
)

type ExpandedPostingsCacheMetrics struct {
	CacheRequests *prometheus.CounterVec
	CacheHits     *prometheus.CounterVec
	CacheEvicts   *prometheus.CounterVec
}

func NewPostingCacheMetrics(r prometheus.Registerer) *ExpandedPostingsCacheMetrics {
	return &ExpandedPostingsCacheMetrics{
		CacheRequests: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_expanded_postings_cache_requests",
			Help: "Count of cache adds in the ingester postings cache.",
		}, []string{"cache"}),
		CacheHits: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_expanded_postings_cache_hits",
			Help: "Count of cache hits in the ingester postings cache.",
		}, []string{"cache"}),
		CacheEvicts: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_expanded_postings_cache_evicts",
			Help: "Count of cache evictions in the ingester postings cache, excluding items that got evicted due to TTL.",
		}, []string{"cache"}),
	}
}

type TSDBPostingsCacheConfig struct {
	Head   PostingsCacheConfig `yaml:"head"`
	Blocks PostingsCacheConfig `yaml:"blocks"`

	PostingsForMatchers func(ctx context.Context, ix tsdb.IndexReader, ms ...*labels.Matcher) (index.Postings, error) `yaml:"-"`
	timeNow             func() time.Time                                                                              `yaml:"-"`
}

type PostingsCacheConfig struct {
	MaxBytes int64         `yaml:"max_bytes"`
	MaxItems int           `yaml:"max_items"`
	Ttl      time.Duration `yaml:"ttl"`
	Enabled  bool          `yaml:"enabled"`
}

func (cfg *TSDBPostingsCacheConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.Head.RegisterFlagsWithPrefix(prefix, "head", f)
	cfg.Blocks.RegisterFlagsWithPrefix(prefix, "block", f)
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet
func (cfg *PostingsCacheConfig) RegisterFlagsWithPrefix(prefix, block string, f *flag.FlagSet) {
	f.Int64Var(&cfg.MaxBytes, prefix+"postings-cache."+block+".max-bytes", 10*1024*1024, "Max bytes for postings cache")
	f.IntVar(&cfg.MaxItems, prefix+"postings-cache."+block+".max-items", 10000, "Max items for postings cache")
	f.DurationVar(&cfg.Ttl, prefix+"postings-cache."+block+".ttl", 10*time.Minute, "TTL for postings cache")
	f.BoolVar(&cfg.Enabled, prefix+"postings-cache."+block+".enabled", false, "Whether the postings cache is enabled or not")
}

type ExpandedPostingsCache interface {
	PostingsForMatchers(ctx context.Context, blockID ulid.ULID, ix tsdb.IndexReader, ms ...*labels.Matcher) (index.Postings, error)
	ExpireSeries(metric labels.Labels)
}

type BlocksPostingsForMatchersCache struct {
	strippedLock []sync.RWMutex

	headCache   *fifoCache[*postingsPromise]
	blocksCache *fifoCache[*postingsPromise]

	headSeedByMetricName    []int
	postingsForMatchersFunc func(ctx context.Context, ix tsdb.IndexReader, ms ...*labels.Matcher) (index.Postings, error)
	timeNow                 func() time.Time

	metrics *ExpandedPostingsCacheMetrics
}

func NewBlocksPostingsForMatchersCache(cfg TSDBPostingsCacheConfig, metrics *ExpandedPostingsCacheMetrics) ExpandedPostingsCache {
	if cfg.PostingsForMatchers == nil {
		cfg.PostingsForMatchers = tsdb.PostingsForMatchers
	}

	if cfg.timeNow == nil {
		cfg.timeNow = time.Now
	}

	return &BlocksPostingsForMatchersCache{
		headCache:               newFifoCache[*postingsPromise](cfg.Head, "head", metrics, cfg.timeNow),
		blocksCache:             newFifoCache[*postingsPromise](cfg.Blocks, "block", metrics, cfg.timeNow),
		headSeedByMetricName:    make([]int, seedArraySize),
		strippedLock:            make([]sync.RWMutex, numOfSeedsStripes),
		postingsForMatchersFunc: cfg.PostingsForMatchers,
		timeNow:                 cfg.timeNow,
		metrics:                 metrics,
	}
}

func (c *BlocksPostingsForMatchersCache) ExpireSeries(metric labels.Labels) {
	metricName, err := extract.MetricNameFromLabels(metric)
	if err != nil {
		return
	}

	h := MemHashString(metricName)
	i := h % uint64(len(c.headSeedByMetricName))
	l := h % uint64(len(c.strippedLock))
	c.strippedLock[l].Lock()
	defer c.strippedLock[l].Unlock()
	c.headSeedByMetricName[i]++
}

func (c *BlocksPostingsForMatchersCache) PostingsForMatchers(ctx context.Context, blockID ulid.ULID, ix tsdb.IndexReader, ms ...*labels.Matcher) (index.Postings, error) {
	return c.fetchPostings(blockID, ix, ms...)(ctx)
}

func (c *BlocksPostingsForMatchersCache) fetchPostings(blockID ulid.ULID, ix tsdb.IndexReader, ms ...*labels.Matcher) func(context.Context) (index.Postings, error) {
	var seed string
	cache := c.blocksCache

	// If is a head block, lets add the seed on the cache key so we can
	// invalidate the cache when new series are created for this metric name
	if isHeadBlock(blockID) {
		cache = c.headCache
		metricName, ok := metricNameFromMatcher(ms)
		// Lets not cache head if we don;t find an equal matcher for the label __name__
		if !ok {
			return func(ctx context.Context) (index.Postings, error) {
				return tsdb.PostingsForMatchers(ctx, ix, ms...)
			}
		}

		seed = c.getSeedForMetricName(metricName)
	}

	// Let's bypass cache if not enabled
	if !cache.cfg.Enabled {
		return func(ctx context.Context) (index.Postings, error) {
			return tsdb.PostingsForMatchers(ctx, ix, ms...)
		}
	}

	c.metrics.CacheRequests.WithLabelValues(cache.name).Inc()

	key := c.cacheKey(seed, blockID, ms...)

	promise := &postingsPromise{
		done: make(chan struct{}),
	}
	oldPromise, loaded := cache.getOrStore(key, promise)
	if loaded {
		c.metrics.CacheHits.WithLabelValues(cache.name).Inc()
		close(promise.done)
		return func(ctx context.Context) (index.Postings, error) {
			return oldPromise.result(ctx)
		}
	}
	defer close(promise.done)
	// Use context.Background() as this promise is maybe shared across calls
	postings, err := c.postingsForMatchersFunc(context.Background(), ix, ms...)

	if err == nil {
		promise.ids, promise.err = index.ExpandPostings(postings)
		sizeBytes := int64(len(key)) + int64(len(promise.ids)*8)
		cache.created(key, c.timeNow(), sizeBytes)
	}

	return promise.result
}

func (c *BlocksPostingsForMatchersCache) getSeedForMetricName(metricName string) string {
	h := MemHashString(metricName)
	i := h % uint64(len(c.headSeedByMetricName))
	l := h % uint64(len(c.strippedLock))
	c.strippedLock[l].RLock()
	defer c.strippedLock[l].RUnlock()
	return strconv.Itoa(c.headSeedByMetricName[i])
}

func (c *BlocksPostingsForMatchersCache) cacheKey(seed string, blockID ulid.ULID, ms ...*labels.Matcher) string {
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
	if !c.shouldEvictHead() {
		c.cachedMtx.RUnlock()
		return
	}
	c.cachedMtx.RUnlock()
	c.cachedMtx.Lock()
	defer c.cachedMtx.Unlock()
	for c.shouldEvictHead() {
		c.evictHead()
	}
}

func (c *fifoCache[V]) getOrStore(k string, v V) (V, bool) {
	c.expire()
	if !c.cfg.Enabled {
		return v, false
	}

	loaded, ok := c.cachedValues.LoadOrStore(k, v)
	return loaded.(V), ok
}

func (c *fifoCache[V]) contains(k string) bool {
	_, ok := c.cachedValues.Load(k)
	return ok
}

func (c *fifoCache[V]) shouldEvictHead() bool {
	if c.cached.Len() > c.cfg.MaxItems || c.cachedBytes > c.cfg.MaxBytes {
		c.metrics.CacheEvicts.WithLabelValues(c.name).Inc()
		return true
	}
	h := c.cached.Front()
	if h == nil {
		return false
	}
	ts := h.Value.(*cacheEntry).ts
	r := c.timeNow().Sub(ts)
	return r >= c.cfg.Ttl
}
func (c *fifoCache[V]) evictHead() {
	front := c.cached.Front()
	oldest := front.Value.(*cacheEntry)
	c.cachedValues.Delete(oldest.key)
	c.cached.Remove(front)
	c.cachedBytes -= oldest.sizeBytes
}

func (c *fifoCache[V]) created(key string, ts time.Time, sizeBytes int64) {
	if c.cfg.Ttl <= 0 {
		c.cachedValues.Delete(key)
		return
	}
	c.cachedMtx.Lock()
	defer c.cachedMtx.Unlock()
	c.cached.PushBack(&cacheEntry{
		key:       key,
		ts:        ts,
		sizeBytes: sizeBytes,
	})
	c.cachedBytes += sizeBytes
}

type cacheEntry struct {
	key       string
	ts        time.Time
	sizeBytes int64
}

type postingsPromise struct {
	done chan struct{}

	ids []storage.SeriesRef
	err error
}

func (p *postingsPromise) result(ctx context.Context) (index.Postings, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.done:
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		return index.NewListPostings(p.ids), p.err
	}
}

func MemHashString(str string) uint64 {
	return xxhash.Sum64(yoloBuf(str))
}
