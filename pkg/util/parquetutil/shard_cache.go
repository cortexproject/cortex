package parquetutil

import (
	"flag"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	defaultMaintenanceInterval = time.Minute
)

type CacheInterface[T any] interface {
	Get(path string) T
	Set(path string, reader T)
	Close()
}

type cacheMetrics struct {
	hits      *prometheus.CounterVec
	misses    *prometheus.CounterVec
	evictions *prometheus.CounterVec
	size      *prometheus.GaugeVec
}

func newCacheMetrics(reg prometheus.Registerer) *cacheMetrics {
	return &cacheMetrics{
		hits: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_parquet_cache_hits_total",
			Help: "Total number of parquet cache hits",
		}, []string{"name"}),
		misses: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_parquet_cache_misses_total",
			Help: "Total number of parquet cache misses",
		}, []string{"name"}),
		evictions: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_parquet_cache_evictions_total",
			Help: "Total number of parquet cache evictions",
		}, []string{"name"}),
		size: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_parquet_cache_item_count",
			Help: "Current number of cached parquet items",
		}, []string{"name"}),
	}
}

type cacheEntry[T any] struct {
	value     T
	expiresAt time.Time
}

type ParquetShardCache[T any] struct {
	cache   *lru.Cache[string, *cacheEntry[T]]
	name    string
	metrics *cacheMetrics
	ttl     time.Duration
	stopCh  chan struct{}
}

type CacheConfig struct {
	ParquetShardCacheSize int           `yaml:"parquet_shard_cache_size"`
	ParquetShardCacheTTL  time.Duration `yaml:"parquet_shard_cache_ttl"`
	MaintenanceInterval   time.Duration `yaml:"-"`
}

func (cfg *CacheConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.IntVar(&cfg.ParquetShardCacheSize, prefix+"parquet-shard-cache-size", 512, "[Experimental] Maximum size of the Parquet shard cache. 0 to disable.")
	f.DurationVar(&cfg.ParquetShardCacheTTL, prefix+"parquet-shard-cache-ttl", 24*time.Hour, "[Experimental] TTL of the Parquet shard cache. 0 to no TTL.")
	cfg.MaintenanceInterval = defaultMaintenanceInterval
}

func NewParquetShardCache[T any](cfg *CacheConfig, name string, reg prometheus.Registerer) (CacheInterface[T], error) {
	if cfg.ParquetShardCacheSize <= 0 {
		return &noopCache[T]{}, nil
	}
	metrics := newCacheMetrics(reg)
	cache, err := lru.NewWithEvict(cfg.ParquetShardCacheSize, func(key string, value *cacheEntry[T]) {
		metrics.evictions.WithLabelValues(name).Inc()
		metrics.size.WithLabelValues(name).Dec()
	})
	if err != nil {
		return nil, err
	}

	c := &ParquetShardCache[T]{
		cache:   cache,
		name:    name,
		metrics: metrics,
		ttl:     cfg.ParquetShardCacheTTL,
		stopCh:  make(chan struct{}),
	}

	if cfg.ParquetShardCacheTTL > 0 {
		go c.maintenanceLoop(cfg.MaintenanceInterval)
	}

	return c, nil
}

func (c *ParquetShardCache[T]) maintenanceLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now()
			keys := c.cache.Keys()
			for _, key := range keys {
				if entry, ok := c.cache.Peek(key); ok {
					// we use a Peek() because the Get() change LRU order.
					if !entry.expiresAt.IsZero() && now.After(entry.expiresAt) {
						c.cache.Remove(key)
					}
				}
			}
		case <-c.stopCh:
			return
		}
	}
}

func (c *ParquetShardCache[T]) Get(path string) (r T) {
	if entry, ok := c.cache.Get(path); ok {
		isExpired := !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt)

		if isExpired {
			c.cache.Remove(path)
			c.metrics.misses.WithLabelValues(c.name).Inc()
			return
		}

		c.metrics.hits.WithLabelValues(c.name).Inc()
		return entry.value
	}
	c.metrics.misses.WithLabelValues(c.name).Inc()
	return
}

func (c *ParquetShardCache[T]) Set(path string, reader T) {
	if !c.cache.Contains(path) {
		c.metrics.size.WithLabelValues(c.name).Inc()
	}

	var expiresAt time.Time
	if c.ttl > 0 {
		expiresAt = time.Now().Add(c.ttl)
	}

	entry := &cacheEntry[T]{
		value:     reader,
		expiresAt: expiresAt,
	}

	c.cache.Add(path, entry)
}

func (c *ParquetShardCache[T]) Close() {
	close(c.stopCh)
}

type noopCache[T any] struct {
}

func (n noopCache[T]) Get(_ string) (r T) {
	return
}

func (n noopCache[T]) Set(_ string, _ T) {

}

func (n noopCache[T]) Close() {}
