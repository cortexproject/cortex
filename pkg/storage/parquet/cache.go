package storage

import (
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Cache[T any] struct {
	cache   *lru.Cache[string, T]
	metrics *CacheMetrics
	name    string
}

type CacheMetrics struct {
	hits      *prometheus.CounterVec
	misses    *prometheus.CounterVec
	evictions *prometheus.CounterVec
	size      *prometheus.GaugeVec
}

func NewCacheMetrics(reg prometheus.Registerer) *CacheMetrics {
	return &CacheMetrics{
		hits: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_parquet_reader_cache_hits_total",
			Help: "Total number of parquet reader cache hits",
		}, []string{"name"}),
		misses: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_parquet_reader_cache_misses_total",
			Help: "Total number of parquet reader cache misses",
		}, []string{"name"}),
		evictions: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_parquet_reader_cache_evictions_total",
			Help: "Total number of parquet reader cache evictions",
		}, []string{"name"}),
		size: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_parquet_reader_cache_size",
			Help: "Current number of cached parquet readers",
		}, []string{"name"}),
	}
}

func NewCache[T any](name string, size int, metrics *CacheMetrics) (*Cache[T], error) {
	cache, err := lru.NewWithEvict(size, func(key string, value T) {
		metrics.evictions.WithLabelValues(name).Inc()
		metrics.size.WithLabelValues(name).Dec()
	})
	if err != nil {
		return nil, err
	}

	return &Cache[T]{
		cache:   cache,
		name:    name,
		metrics: metrics,
	}, nil
}

func (c *Cache[T]) Get(path string) (r T) {
	if reader, ok := c.cache.Get(path); ok {
		c.metrics.hits.WithLabelValues(c.name).Inc()
		return reader
	}
	c.metrics.misses.WithLabelValues(c.name).Inc()
	return
}

func (c *Cache[T]) Set(path string, reader T) {
	if !c.cache.Contains(path) {
		c.metrics.size.WithLabelValues(c.name).Inc()
	}
	c.cache.Add(path, reader)
}

func (c *Cache[T]) Remove(path string) {
	if c.cache.Contains(path) {
		c.cache.Remove(path)
		c.metrics.size.WithLabelValues(c.name).Dec()
	}
}
