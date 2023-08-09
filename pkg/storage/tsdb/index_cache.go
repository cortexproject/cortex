package tsdb

import (
	"flag"
	"fmt"
	"strings"

	"github.com/alecthomas/units"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/cacheutil"
	"github.com/thanos-io/thanos/pkg/model"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"

	"github.com/cortexproject/cortex/pkg/util"
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
)

var (
	supportedIndexCacheBackends = []string{IndexCacheBackendInMemory, IndexCacheBackendMemcached, IndexCacheBackendRedis}

	errUnsupportedIndexCacheBackend = errors.New("unsupported index cache backend")
	errDuplicatedIndexCacheBackend  = errors.New("duplicated index cache backend")
	errNoIndexCacheAddresses        = errors.New("no index cache backend addresses")
)

type IndexCacheConfig struct {
	Backend   string                   `yaml:"backend"`
	InMemory  InMemoryIndexCacheConfig `yaml:"inmemory"`
	Memcached MemcachedClientConfig    `yaml:"memcached"`
	Redis     RedisClientConfig        `yaml:"redis"`
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
}

// Validate the config.
func (cfg *IndexCacheConfig) Validate() error {

	splitBackends := strings.Split(cfg.Backend, ",")
	configuredBackends := map[string]struct{}{}

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
		}

		configuredBackends[backend] = struct{}{}
	}

	return nil
}

type InMemoryIndexCacheConfig struct {
	MaxSizeBytes uint64 `yaml:"max_size_bytes"`
}

func (cfg *InMemoryIndexCacheConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.Uint64Var(&cfg.MaxSizeBytes, prefix+"max-size-bytes", uint64(1*units.Gibibyte), "Maximum size in bytes of in-memory index cache used to speed up blocks index lookups (shared between all tenants).")
}

// NewIndexCache creates a new index cache based on the input configuration.
func NewIndexCache(cfg IndexCacheConfig, logger log.Logger, registerer prometheus.Registerer) (storecache.IndexCache, error) {
	splitBackends := strings.Split(cfg.Backend, ",")
	var caches []storecache.IndexCache

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
		case IndexCacheBackendMemcached:
			c, err := newMemcachedIndexCacheClient(cfg.Memcached, logger, registerer)
			if err != nil {
				return nil, err
			}
			cache, err := storecache.NewRemoteIndexCache(logger, c, nil, iReg)
			if err != nil {
				return nil, err
			}
			caches = append(caches, cache)
		case IndexCacheBackendRedis:
			c, err := newRedisIndexCacheClient(cfg.Redis, logger, iReg)
			if err != nil {
				return nil, err
			}
			cache, err := storecache.NewRemoteIndexCache(logger, c, nil, iReg)
			if err != nil {
				return nil, err
			}
			caches = append(caches, cache)
		default:
			return nil, errUnsupportedIndexCacheBackend
		}
	}

	return newMultiLevelCache(caches...), nil
}

func newInMemoryIndexCache(cfg InMemoryIndexCacheConfig, logger log.Logger, registerer prometheus.Registerer) (storecache.IndexCache, error) {
	maxCacheSize := model.Bytes(cfg.MaxSizeBytes)

	// Calculate the max item size.
	maxItemSize := defaultMaxItemSize
	if maxItemSize > maxCacheSize {
		maxItemSize = maxCacheSize
	}

	return storecache.NewInMemoryIndexCacheWithConfig(logger, nil, registerer, storecache.InMemoryIndexCacheConfig{
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
