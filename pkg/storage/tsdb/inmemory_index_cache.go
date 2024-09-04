package tsdb

import (
	"context"
	"unsafe"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/tenancy"

	"github.com/cortexproject/cortex/pkg/util"
)

type InMemoryIndexCache struct {
	logger           log.Logger
	cache            *fastcache.Cache
	maxItemSizeBytes uint64

	added    *prometheus.CounterVec
	overflow *prometheus.CounterVec

	commonMetrics *storecache.CommonMetrics
}

// NewInMemoryIndexCacheWithConfig creates a new thread-safe cache for index entries. It relies on the cache library
// (fastcache) to ensures the total cache size approximately does not exceed maxBytes.
func NewInMemoryIndexCacheWithConfig(logger log.Logger, commonMetrics *storecache.CommonMetrics, reg prometheus.Registerer, config storecache.InMemoryIndexCacheConfig) (*InMemoryIndexCache, error) {
	if config.MaxItemSize > config.MaxSize {
		return nil, errors.Errorf("max item size (%v) cannot be bigger than overall cache size (%v)", config.MaxItemSize, config.MaxSize)
	}

	// fastcache will panic if MaxSize <= 0.
	if config.MaxSize <= 0 {
		config.MaxSize = storecache.DefaultInMemoryIndexCacheConfig.MaxSize
	}

	if commonMetrics == nil {
		commonMetrics = storecache.NewCommonMetrics(reg)
	}

	c := &InMemoryIndexCache{
		logger:           logger,
		maxItemSizeBytes: uint64(config.MaxItemSize),
		commonMetrics:    commonMetrics,
	}

	c.added = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_items_added_total",
		Help: "Total number of items that were added to the index cache.",
	}, []string{"item_type"})
	c.added.WithLabelValues(storecache.CacheTypePostings)
	c.added.WithLabelValues(storecache.CacheTypeSeries)
	c.added.WithLabelValues(storecache.CacheTypeExpandedPostings)

	c.commonMetrics.RequestTotal.WithLabelValues(storecache.CacheTypePostings, tenancy.DefaultTenant)
	c.commonMetrics.RequestTotal.WithLabelValues(storecache.CacheTypeSeries, tenancy.DefaultTenant)
	c.commonMetrics.RequestTotal.WithLabelValues(storecache.CacheTypeExpandedPostings, tenancy.DefaultTenant)

	c.overflow = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_items_overflowed_total",
		Help: "Total number of items that could not be added to the cache due to being too big.",
	}, []string{"item_type"})
	c.overflow.WithLabelValues(storecache.CacheTypePostings)
	c.overflow.WithLabelValues(storecache.CacheTypeSeries)
	c.overflow.WithLabelValues(storecache.CacheTypeExpandedPostings)

	c.commonMetrics.HitsTotal.WithLabelValues(storecache.CacheTypePostings, tenancy.DefaultTenant)
	c.commonMetrics.HitsTotal.WithLabelValues(storecache.CacheTypeSeries, tenancy.DefaultTenant)
	c.commonMetrics.HitsTotal.WithLabelValues(storecache.CacheTypeExpandedPostings, tenancy.DefaultTenant)

	c.cache = fastcache.New(int(config.MaxSize))
	level.Info(logger).Log(
		"msg", "created in-memory index cache",
		"maxItemSizeBytes", c.maxItemSizeBytes,
		"maxSizeBytes", config.MaxSize,
	)
	return c, nil
}

func (c *InMemoryIndexCache) get(key storecache.CacheKey) ([]byte, bool) {
	k := yoloBuf(key.String())
	resp := c.cache.GetBig(nil, k)
	if len(resp) == 0 {
		return nil, false
	}
	return resp, true
}

func (c *InMemoryIndexCache) set(typ string, key storecache.CacheKey, val []byte) {
	k := yoloBuf(key.String())
	r := c.cache.GetBig(nil, k)
	// item exists, no need to set it again.
	if r != nil {
		return
	}

	size := uint64(len(k) + len(val))
	if size > c.maxItemSizeBytes {
		level.Info(c.logger).Log(
			"msg", "item bigger than maxItemSizeBytes. Ignoring..",
			"maxItemSizeBytes", c.maxItemSizeBytes,
			"cacheType", typ,
		)
		c.overflow.WithLabelValues(typ).Inc()
		return
	}

	c.cache.SetBig(k, val)
	c.added.WithLabelValues(typ).Inc()
}

func yoloBuf(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}

func copyString(s string) string {
	return string(unsafe.Slice(unsafe.StringData(s), len(s)))
}

// copyToKey is required as underlying strings might be memory-mapped.
func copyToKey(l labels.Label) storecache.CacheKeyPostings {
	return storecache.CacheKeyPostings(labels.Label{Value: copyString(l.Value), Name: copyString(l.Name)})
}

// StorePostings sets the postings identified by the ulid and label to the value v,
// if the postings already exists in the cache it is not mutated.
func (c *InMemoryIndexCache) StorePostings(blockID ulid.ULID, l labels.Label, v []byte, tenant string) {
	c.commonMetrics.DataSizeBytes.WithLabelValues(storecache.CacheTypePostings, tenant).Observe(float64(len(v)))
	c.set(storecache.CacheTypePostings, storecache.CacheKey{Block: blockID.String(), Key: copyToKey(l)}, v)
}

// FetchMultiPostings fetches multiple postings - each identified by a label -
// and returns a map containing cache hits, along with a list of missing keys.
func (c *InMemoryIndexCache) FetchMultiPostings(ctx context.Context, blockID ulid.ULID, keys []labels.Label, tenant string) (hits map[labels.Label][]byte, misses []labels.Label) {
	timer := prometheus.NewTimer(c.commonMetrics.FetchLatency.WithLabelValues(storecache.CacheTypePostings, tenant))
	defer timer.ObserveDuration()

	hits = map[labels.Label][]byte{}

	blockIDKey := blockID.String()
	requests := 0
	hit := 0
	for i, key := range keys {
		if (i+1)%util.CheckContextEveryNIterations == 0 && ctx.Err() != nil {
			c.commonMetrics.RequestTotal.WithLabelValues(storecache.CacheTypePostings, tenant).Add(float64(requests))
			c.commonMetrics.HitsTotal.WithLabelValues(storecache.CacheTypePostings, tenant).Add(float64(hit))
			return hits, misses
		}
		requests++
		if b, ok := c.get(storecache.CacheKey{Block: blockIDKey, Key: storecache.CacheKeyPostings(key)}); ok {
			hit++
			hits[key] = b
			continue
		}

		misses = append(misses, key)
	}
	c.commonMetrics.RequestTotal.WithLabelValues(storecache.CacheTypePostings, tenant).Add(float64(requests))
	c.commonMetrics.HitsTotal.WithLabelValues(storecache.CacheTypePostings, tenant).Add(float64(hit))

	return hits, misses
}

// StoreExpandedPostings stores expanded postings for a set of label matchers.
func (c *InMemoryIndexCache) StoreExpandedPostings(blockID ulid.ULID, matchers []*labels.Matcher, v []byte, tenant string) {
	c.commonMetrics.DataSizeBytes.WithLabelValues(storecache.CacheTypeExpandedPostings, tenant).Observe(float64(len(v)))
	c.set(storecache.CacheTypeExpandedPostings, storecache.CacheKey{Block: blockID.String(), Key: storecache.CacheKeyExpandedPostings(storecache.LabelMatchersToString(matchers))}, v)
}

// FetchExpandedPostings fetches expanded postings and returns cached data and a boolean value representing whether it is a cache hit or not.
func (c *InMemoryIndexCache) FetchExpandedPostings(ctx context.Context, blockID ulid.ULID, matchers []*labels.Matcher, tenant string) ([]byte, bool) {
	timer := prometheus.NewTimer(c.commonMetrics.FetchLatency.WithLabelValues(storecache.CacheTypeExpandedPostings, tenant))
	defer timer.ObserveDuration()

	if ctx.Err() != nil {
		return nil, false
	}
	c.commonMetrics.RequestTotal.WithLabelValues(storecache.CacheTypeExpandedPostings, tenant).Inc()
	if b, ok := c.get(storecache.CacheKey{Block: blockID.String(), Key: storecache.CacheKeyExpandedPostings(storecache.LabelMatchersToString(matchers))}); ok {
		c.commonMetrics.HitsTotal.WithLabelValues(storecache.CacheTypeExpandedPostings, tenant).Inc()
		return b, true
	}
	return nil, false
}

// StoreSeries sets the series identified by the ulid and id to the value v,
// if the series already exists in the cache it is not mutated.
func (c *InMemoryIndexCache) StoreSeries(blockID ulid.ULID, id storage.SeriesRef, v []byte, tenant string) {
	c.commonMetrics.DataSizeBytes.WithLabelValues(storecache.CacheTypeSeries, tenant).Observe(float64(len(v)))
	c.set(storecache.CacheTypeSeries, storecache.CacheKey{Block: blockID.String(), Key: storecache.CacheKeySeries(id)}, v)
}

// FetchMultiSeries fetches multiple series - each identified by ID - from the cache
// and returns a map containing cache hits, along with a list of missing IDs.
func (c *InMemoryIndexCache) FetchMultiSeries(ctx context.Context, blockID ulid.ULID, ids []storage.SeriesRef, tenant string) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef) {
	timer := prometheus.NewTimer(c.commonMetrics.FetchLatency.WithLabelValues(storecache.CacheTypeSeries, tenant))
	defer timer.ObserveDuration()

	hits = map[storage.SeriesRef][]byte{}

	blockIDKey := blockID.String()
	requests := 0
	hit := 0
	for i, id := range ids {
		if (i+1)%util.CheckContextEveryNIterations == 0 && ctx.Err() != nil {
			c.commonMetrics.RequestTotal.WithLabelValues(storecache.CacheTypeSeries, tenant).Add(float64(requests))
			c.commonMetrics.HitsTotal.WithLabelValues(storecache.CacheTypeSeries, tenant).Add(float64(hit))
			return hits, misses
		}
		requests++
		if b, ok := c.get(storecache.CacheKey{Block: blockIDKey, Key: storecache.CacheKeySeries(id)}); ok {
			hit++
			hits[id] = b
			continue
		}

		misses = append(misses, id)
	}
	c.commonMetrics.RequestTotal.WithLabelValues(storecache.CacheTypeSeries, tenant).Add(float64(requests))
	c.commonMetrics.HitsTotal.WithLabelValues(storecache.CacheTypeSeries, tenant).Add(float64(hit))

	return hits, misses
}
