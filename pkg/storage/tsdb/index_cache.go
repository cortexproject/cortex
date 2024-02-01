package tsdb

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/alecthomas/units"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/cacheutil"
	"github.com/thanos-io/thanos/pkg/model"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

const (
	// IndexCacheBackendInMemory is the value for the in-memory index cache backend.
	IndexCacheBackendInMemory = "inmemory"

	// IndexCacheBackendMemcached is the value for the memcached index cache backend.
	IndexCacheBackendMemcached = "memcached"

	// IndexCacheBackendRedis is the value for the redis index cache backend.
	IndexCacheBackendRedis = "redis"

	// IndexCacheBackendDefault is the value for the default index cache backend.
	IndexCacheBackendDefault = IndexCacheBackendInMemory

	defaultMaxItemSize = model.Bytes(128 * units.MiB)

	defaultTTL = 24 * time.Hour
)

var (
	supportedIndexCacheBackends = []string{IndexCacheBackendInMemory, IndexCacheBackendMemcached, IndexCacheBackendRedis}

	errUnsupportedIndexCacheBackend = errors.New("unsupported index cache backend")
	errDuplicatedIndexCacheBackend  = errors.New("duplicated index cache backend")
	errNoIndexCacheAddresses        = errors.New("no index cache backend addresses")
	errInvalidMaxAsyncConcurrency   = errors.New("invalid max_async_concurrency, must greater than 0")
	errInvalidMaxAsyncBufferSize    = errors.New("invalid max_async_buffer_size, must greater than 0")
	errInvalidMaxBackfillItems      = errors.New("invalid max_backfill_items, must greater than 0")
)

type IndexCacheConfig struct {
	Backend    string                     `yaml:"backend"`
	InMemory   InMemoryIndexCacheConfig   `yaml:"inmemory"`
	Memcached  MemcachedIndexCacheConfig  `yaml:"memcached"`
	Redis      RedisIndexCacheConfig      `yaml:"redis"`
	MultiLevel MultiLevelIndexCacheConfig `yaml:"multilevel"`
}

func (cfg *IndexCacheConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix(f, "blocks-storage.bucket-store.index-cache.")
}

func (cfg *IndexCacheConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Backend, prefix+"backend", IndexCacheBackendDefault, fmt.Sprintf("The index cache backend type. "+
		"Multiple cache backend can be provided as a comma-separated ordered list to enable the implementation of a cache hierarchy. "+
		"Supported values: %s.",
		strings.Join(supportedIndexCacheBackends, ", ")))

	cfg.InMemory.RegisterFlagsWithPrefix(f, prefix+"inmemory.")
	cfg.Memcached.RegisterFlagsWithPrefix(f, prefix+"memcached.")
	cfg.Redis.RegisterFlagsWithPrefix(f, prefix+"redis.")
	cfg.MultiLevel.RegisterFlagsWithPrefix(f, prefix+"multilevel.")
}

// Validate the config.
func (cfg *IndexCacheConfig) Validate() error {

	splitBackends := strings.Split(cfg.Backend, ",")
	configuredBackends := map[string]struct{}{}

	if len(splitBackends) > 1 {
		if err := cfg.MultiLevel.Validate(); err != nil {
			return err
		}
	}

	for _, backend := range splitBackends {
		if !util.StringsContain(supportedIndexCacheBackends, backend) {
			return errUnsupportedIndexCacheBackend
		}

		if _, ok := configuredBackends[backend]; ok {
			return errors.WithMessagef(errDuplicatedIndexCacheBackend, "duplicated backend: %v", backend)
		}

		if backend == IndexCacheBackendMemcached {
			if err := cfg.Memcached.Validate(); err != nil {
				return err
			}
		} else if backend == IndexCacheBackendRedis {
			if err := cfg.Redis.Validate(); err != nil {
				return err
			}
		} else {
			if err := cfg.InMemory.Validate(); err != nil {
				return err
			}
		}

		configuredBackends[backend] = struct{}{}
	}

	return nil
}

type MultiLevelIndexCacheConfig struct {
	MaxAsyncConcurrency int `yaml:"max_async_concurrency"`
	MaxAsyncBufferSize  int `yaml:"max_async_buffer_size"`
	MaxBackfillItems    int `yaml:"max_backfill_items"`
}

func (cfg *MultiLevelIndexCacheConfig) Validate() error {
	if cfg.MaxAsyncBufferSize <= 0 {
		return errInvalidMaxAsyncBufferSize
	}
	if cfg.MaxAsyncConcurrency <= 0 {
		return errInvalidMaxAsyncConcurrency
	}
	if cfg.MaxBackfillItems <= 0 {
		return errInvalidMaxBackfillItems
	}
	return nil
}

func (cfg *MultiLevelIndexCacheConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.IntVar(&cfg.MaxAsyncConcurrency, prefix+"max-async-concurrency", 50, "The maximum number of concurrent asynchronous operations can occur when backfilling cache items.")
	f.IntVar(&cfg.MaxAsyncBufferSize, prefix+"max-async-buffer-size", 10000, "The maximum number of enqueued asynchronous operations allowed when backfilling cache items.")
	f.IntVar(&cfg.MaxBackfillItems, prefix+"max-backfill-items", 10000, "The maximum number of items to backfill per asynchronous operation.")
}

type InMemoryIndexCacheConfig struct {
	MaxSizeBytes uint64   `yaml:"max_size_bytes"`
	EnabledItems []string `yaml:"enabled_items"`
}

func (cfg *InMemoryIndexCacheConfig) Validate() error {
	if err := storecache.ValidateEnabledItems(cfg.EnabledItems); err != nil {
		return err
	}
	return nil
}

func (cfg *InMemoryIndexCacheConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.Uint64Var(&cfg.MaxSizeBytes, prefix+"max-size-bytes", uint64(1*units.Gibibyte), "Maximum size in bytes of in-memory index cache used to speed up blocks index lookups (shared between all tenants).")
	f.Var((*flagext.StringSlice)(&cfg.EnabledItems), prefix+"enabled-items", "Selectively cache index item types. Supported values are Postings, ExpandedPostings and Series")
}

type MemcachedIndexCacheConfig struct {
	ClientConfig MemcachedClientConfig `yaml:",inline"`
	EnabledItems []string              `yaml:"enabled_items"`
}

func (cfg *MemcachedIndexCacheConfig) Validate() error {
	if err := cfg.ClientConfig.Validate(); err != nil {
		return err
	}
	return storecache.ValidateEnabledItems(cfg.EnabledItems)
}

func (cfg *MemcachedIndexCacheConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	cfg.ClientConfig.RegisterFlagsWithPrefix(f, prefix)
	f.Var((*flagext.StringSlice)(&cfg.EnabledItems), prefix+"enabled-items", "Selectively cache index item types. Supported values are Postings, ExpandedPostings and Series")
}

type RedisIndexCacheConfig struct {
	ClientConfig RedisClientConfig `yaml:",inline"`
	EnabledItems []string          `yaml:"enabled_items"`
}

func (cfg *RedisIndexCacheConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	cfg.ClientConfig.RegisterFlagsWithPrefix(f, prefix)
	f.Var((*flagext.StringSlice)(&cfg.EnabledItems), prefix+"enabled-items", "Selectively cache index item types. Supported values are Postings, ExpandedPostings and Series")
}

func (cfg *RedisIndexCacheConfig) Validate() error {
	if err := cfg.ClientConfig.Validate(); err != nil {
		return err
	}
	return storecache.ValidateEnabledItems(cfg.EnabledItems)
}

// NewIndexCache creates a new index cache based on the input configuration.
func NewIndexCache(cfg IndexCacheConfig, logger log.Logger, registerer prometheus.Registerer) (storecache.IndexCache, error) {
	splitBackends := strings.Split(cfg.Backend, ",")
	var (
		caches       []storecache.IndexCache
		enabledItems [][]string
	)

	for i, backend := range splitBackends {
		iReg := registerer

		// Create the level label if we have more than one cache
		if len(splitBackends) > 1 {
			iReg = prometheus.WrapRegistererWith(prometheus.Labels{"level": fmt.Sprintf("L%v", i)}, registerer)
		}

		switch backend {
		case IndexCacheBackendInMemory:
			c, err := newInMemoryIndexCache(cfg.InMemory, logger, iReg)
			if err != nil {
				return c, err
			}
			caches = append(caches, c)
			enabledItems = append(enabledItems, cfg.InMemory.EnabledItems)
		case IndexCacheBackendMemcached:
			c, err := newMemcachedIndexCacheClient(cfg.Memcached.ClientConfig, logger, registerer)
			if err != nil {
				return nil, err
			}
			// TODO(yeya24): expose TTL
			cache, err := storecache.NewRemoteIndexCache(logger, c, nil, iReg, defaultTTL)
			if err != nil {
				return nil, err
			}
			caches = append(caches, cache)
			enabledItems = append(enabledItems, cfg.Memcached.EnabledItems)
		case IndexCacheBackendRedis:
			c, err := newRedisIndexCacheClient(cfg.Redis.ClientConfig, logger, registerer)
			if err != nil {
				return nil, err
			}
			// TODO(yeya24): expose TTL
			cache, err := storecache.NewRemoteIndexCache(logger, c, nil, iReg, defaultTTL)
			if err != nil {
				return nil, err
			}
			caches = append(caches, cache)
			enabledItems = append(enabledItems, cfg.Redis.EnabledItems)
		default:
			return nil, errUnsupportedIndexCacheBackend
		}
	}

	return newMultiLevelCache(registerer, cfg.MultiLevel, enabledItems, caches...), nil
}

func newInMemoryIndexCache(cfg InMemoryIndexCacheConfig, logger log.Logger, registerer prometheus.Registerer) (storecache.IndexCache, error) {
	maxCacheSize := model.Bytes(cfg.MaxSizeBytes)

	// Calculate the max item size.
	maxItemSize := defaultMaxItemSize
	if maxItemSize > maxCacheSize {
		maxItemSize = maxCacheSize
	}

	return NewInMemoryIndexCacheWithConfig(logger, nil, registerer, storecache.InMemoryIndexCacheConfig{
		MaxSize:     maxCacheSize,
		MaxItemSize: maxItemSize,
	})
}

func newMemcachedIndexCacheClient(cfg MemcachedClientConfig, logger log.Logger, registerer prometheus.Registerer) (cacheutil.RemoteCacheClient, error) {
	client, err := cacheutil.NewMemcachedClientWithConfig(logger, "index-cache", cfg.ToMemcachedClientConfig(), registerer)
	if err != nil {
		return nil, errors.Wrapf(err, "create index cache memcached client")
	}

	return client, err
}

func newRedisIndexCacheClient(cfg RedisClientConfig, logger log.Logger, registerer prometheus.Registerer) (cacheutil.RemoteCacheClient, error) {
	client, err := cacheutil.NewRedisClientWithConfig(logger, "index-cache", cfg.ToRedisClientConfig(), registerer)
	if err != nil {
		return nil, errors.Wrapf(err, "create index cache redis client")
	}

	return client, err
}
