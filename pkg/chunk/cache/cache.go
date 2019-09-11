package cache

import (
	"context"
	"flag"
	"fmt"
	"time"
)

// Cache byte arrays by key.
//
// NB we intentionally do not return errors in this interface - caching is best
// effort by definition.  We found that when these methods did return errors,
// the caller would just log them - so its easier for implementation to do that.
// Whatsmore, we found partially successful Fetchs were often treated as failed
// when they returned an error.
type Cache interface {
	Store(ctx context.Context, key []string, buf [][]byte)
	Fetch(ctx context.Context, keys []string) (found []string, bufs [][]byte, missing []string)
	Stop() error
}

// Supported data storage systems
const (
	memcachedCache = "memcached"
	redisCache     = "redis"
)

// Config for building Caches.
type Config struct {
	EnableFifoCache bool `yaml:"enable_fifocache,omitempty"`

	DefaultValidity time.Duration `yaml:"defaul_validity,omitempty"`

	Background BackgroundConfig `yaml:"background,omitempty"`
	Store      StoreConfig      `yaml:"store,omitempty"`
	Fifocache  FifoCacheConfig  `yaml:"fifocache,omitempty"`

	// backward compatibility for memcached settings (to be deprecated)
	Memcache       MemcachedConfig       `yaml:"memcached,omitempty"`
	MemcacheClient MemcachedClientConfig `yaml:"memcached_client,omitempty"`

	// This is to name the cache metrics properly.
	Prefix string `yaml:"prefix,omitempty"`

	// For tests to inject specific implementations.
	Cache Cache
}

// StoreConfig for configuring data storage systems
type StoreConfig struct {
	Type           string        `yaml:"type"`
	Host           string        `yaml:"host,omitempty"`
	Service        string        `yaml:"service,omitempty"`
	Timeout        time.Duration `yaml:"timeout,omitempty"`
	MaxIdleConns   int           `yaml:"max_idle_conns,omitempty"`
	MaxActiveConns int           `yaml:"max_active_conns,omitempty"`
	UpdateInterval time.Duration `yaml:"update_interval,omitempty"`
	Expiration     time.Duration `yaml:"expiration,omitempty"`
	BatchSize      int           `yaml:"batch_size,omitempty"`
	Parallelism    int           `yaml:"parallelism,omitempty"`
	ConsistentHash bool          `yaml:"consistent_hash,omitempty"`
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, description string, f *flag.FlagSet) {
	cfg.Background.RegisterFlagsWithPrefix(prefix, description, f)
	cfg.Store.RegisterFlagsWithPrefix(prefix, description, f)
	cfg.Fifocache.RegisterFlagsWithPrefix(prefix, description, f)

	// backward compatability for memcached settings (to be deprecated)
	cfg.Memcache.RegisterFlagsWithPrefix(prefix, description, f)
	cfg.MemcacheClient.RegisterFlagsWithPrefix(prefix, description, f)

	f.BoolVar(&cfg.EnableFifoCache, prefix+"cache.enable-fifocache", false, description+"Enable in-memory cache.")
	f.DurationVar(&cfg.DefaultValidity, prefix+"default-validity", 0, description+"The default validity of entries for caches unless overridden.")

	cfg.Prefix = prefix
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet
func (cfg *StoreConfig) RegisterFlagsWithPrefix(prefix, description string, f *flag.FlagSet) {
	f.StringVar(&cfg.Type, prefix+"cache.store.type", "", description+"Type of storage cache.")
	f.DurationVar(&cfg.Expiration, prefix+"cache.store.expiration", 0, description+"How long keys stay in the cache.")
	f.IntVar(&cfg.BatchSize, prefix+"cache.store.batchsize", 0, description+"How many keys to fetch in each batch.")
	f.IntVar(&cfg.Parallelism, prefix+"cache.store.parallelism", 100, description+"Maximum active requests to cache.")
	f.StringVar(&cfg.Host, prefix+"cache.store.hostname", "", description+"Hostname for cache service to use when caching chunks.")
	f.StringVar(&cfg.Service, prefix+"cached.store.service", "", description+"SRV service used to discover cache servers.")
	f.IntVar(&cfg.MaxIdleConns, prefix+"cache.store.max-idle-conns", 16, description+"Maximum number of idle connections in pool.")
	f.IntVar(&cfg.MaxActiveConns, prefix+"cache.store.max-active-conns", 0, description+"Maximum number of active connections in pool.")
	f.DurationVar(&cfg.Timeout, prefix+"cache.store.timeout", 100*time.Millisecond, description+"Maximum time to wait before giving up on memcached requests.")
	f.DurationVar(&cfg.UpdateInterval, prefix+"cache.store.update-interval", 1*time.Minute, description+"Period with which to poll DNS for cache servers (if applicable).")
	f.BoolVar(&cfg.ConsistentHash, prefix+"cached.store.consistent-hash", false, description+"Use consistent hashing to distribute to cache servers (if applicable).")
}

// New creates a new Cache using Config.
func New(cfg Config) (Cache, error) {
	if cfg.Cache != nil {
		return cfg.Cache, nil
	}

	caches := []Cache{}

	if cfg.EnableFifoCache {
		if cfg.Fifocache.Validity == 0 && cfg.DefaultValidity != 0 {
			cfg.Fifocache.Validity = cfg.DefaultValidity
		}

		cache := NewFifoCache(cfg.Prefix+"fifocache", cfg.Fifocache)
		caches = append(caches, Instrument(cfg.Prefix+"fifocache", cache))
	}

	// backward compatability for memcached settings (to be deprecated)
	if len(cfg.Store.Type) == 0 {
		if len(cfg.MemcacheClient.Host) > 0 {
			cfg.Store.Type = memcachedCache
			cfg.Store.Expiration = cfg.Memcache.Expiration
			cfg.Store.BatchSize = cfg.Memcache.BatchSize
			cfg.Store.Parallelism = cfg.Memcache.Parallelism
			cfg.Store.Host = cfg.MemcacheClient.Host
			cfg.Store.Service = cfg.MemcacheClient.Service
			cfg.Store.Timeout = cfg.MemcacheClient.Timeout
			cfg.Store.MaxIdleConns = cfg.MemcacheClient.MaxIdleConns
			cfg.Store.UpdateInterval = cfg.MemcacheClient.UpdateInterval
			cfg.Store.ConsistentHash = cfg.MemcacheClient.ConsistentHash
		}
	}

	if len(cfg.Store.Type) > 0 {
		if cfg.Store.Expiration == 0 && cfg.DefaultValidity != 0 {
			cfg.Store.Expiration = cfg.DefaultValidity
		}
		switch cfg.Store.Type {
		case memcachedCache:
			client := NewMemcachedClient(cfg.Store)
			cache := NewMemcached(cfg.Store, client, cfg.Prefix)
			cacheName := cfg.Prefix + "memcache"
			caches = append(caches, NewBackground(cacheName, cfg.Background, Instrument(cacheName, cache)))
		case redisCache:
			cache := NewRedisCache(cfg.Store, cfg.Prefix, nil)
			cacheName := cfg.Prefix + "redis"
			caches = append(caches, NewBackground(cacheName, cfg.Background, Instrument(cacheName, cache)))
		default:
			return nil, fmt.Errorf("%q storage is not supported", cfg.Store.Type)
		}
	}

	cache := NewTiered(caches)
	if len(caches) > 1 {
		cache = Instrument(cfg.Prefix+"tiered", cache)
	}
	return cache, nil
}
