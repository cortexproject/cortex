package cache

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

// RedisCache type caches chunks in redis
type RedisCache struct {
	name   string
	redis  RedisClient
	logger log.Logger
}

// RedisConfig defines how a RedisCache should be constructed.
type RedisConfig struct {
	Topology             string         `yaml:"topology"`
	Endpoint             string         `yaml:"endpoint"`
	Timeout              time.Duration  `yaml:"timeout"`
	Expiration           time.Duration  `yaml:"expiration"`
	MaxIdleConns         int            `yaml:"max_idle_conns"`
	MaxActiveConns       int            `yaml:"max_active_conns"`
	Password             flagext.Secret `yaml:"password"`
	EnableTLS            bool           `yaml:"enable_tls"`
	IdleTimeout          time.Duration  `yaml:"idle_timeout"`
	WaitOnPoolExhaustion bool           `yaml:"wait_on_pool_exhaustion"`
	MaxConnLifetime      time.Duration  `yaml:"max_conn_lifetime"`
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet
func (cfg *RedisConfig) RegisterFlagsWithPrefix(prefix, description string, f *flag.FlagSet) {
	f.StringVar(&cfg.Topology, prefix+"redis.topology", "server", description+"Redis topology. Supported: 'server', 'cluster', 'sentinel'.")
	f.StringVar(&cfg.Endpoint, prefix+"redis.endpoint", "", description+"Redis service endpoint to use when caching chunks. If empty, no redis will be used.")
	f.DurationVar(&cfg.Timeout, prefix+"redis.timeout", 100*time.Millisecond, description+"Maximum time to wait before giving up on redis requests.")
	f.DurationVar(&cfg.Expiration, prefix+"redis.expiration", 0, description+"How long keys stay in the redis.")
	f.IntVar(&cfg.MaxIdleConns, prefix+"redis.max-idle-conns", 80, description+"Maximum number of idle connections in pool.")
	f.IntVar(&cfg.MaxActiveConns, prefix+"redis.max-active-conns", 0, description+"Maximum number of active connections in pool.")
	f.Var(&cfg.Password, prefix+"redis.password", description+"Password to use when connecting to redis.")
	f.BoolVar(&cfg.EnableTLS, prefix+"redis.enable-tls", false, description+"Enables connecting to redis with TLS.")
	f.DurationVar(&cfg.IdleTimeout, prefix+"redis.idle-timeout", 0, description+"Close connections after remaining idle for this duration. If the value is zero, then idle connections are not closed.")
	f.BoolVar(&cfg.WaitOnPoolExhaustion, prefix+"redis.wait-on-pool-exhaustion", false, description+"Enables waiting if there are no idle connections. If the value is false and the pool is at the max_active_conns limit, the pool will return a connection with ErrPoolExhausted error and not wait for idle connections.")
	f.DurationVar(&cfg.MaxConnLifetime, prefix+"redis.max-conn-lifetime", 0, description+"Close connections older than this duration. If the value is zero, then the pool does not close connections based on age.")
}

// Validate Redis configuration
func (cfg *RedisConfig) Validate() error {
	switch cfg.Topology {
	case redisTopologyServer, redisTopologyCluster, redisTopologySentinel:
	default:
		return fmt.Errorf("unsupported Redis topology %q", cfg.Topology)
	}
	return nil
}

// NewRedisCache creates a new RedisCache
func NewRedisCache(cfg RedisConfig, name string, logger log.Logger, redisClient RedisClient) *RedisCache {
	util.WarnExperimentalUse("Redis cache")
	// pool != nil only in unit tests
	if redisClient == nil {
		redisClient = NewRedisClient(&cfg)
	}
	cache := &RedisCache{
		name:   name,
		redis:  redisClient,
		logger: logger,
	}

	if err := cache.redis.Ping(context.Background()); err != nil {
		level.Error(logger).Log("msg", "error connecting to redis", "endpoint", cfg.Endpoint, "err", err)
	}

	return cache
}

// Fetch gets keys from the cache. The keys that are found must be in the order of the keys requested.
func (c *RedisCache) Fetch(ctx context.Context, keys []string) (found []string, bufs [][]byte, missed []string) {
	data, err := c.redis.MGet(ctx, keys)
	if err != nil {
		level.Error(c.logger).Log("msg", "failed to get from redis", "name", c.name, "err", err)
		missed = make([]string, len(keys))
		copy(missed, keys)
		return
	}
	for i, key := range keys {
		if data[i] != nil {
			found = append(found, key)
			bufs = append(bufs, data[i])
		} else {
			missed = append(missed, key)
		}
	}
	return
}

// Store stores the key in the cache.
func (c *RedisCache) Store(ctx context.Context, keys []string, bufs [][]byte) {
	err := c.redis.MSet(ctx, keys, bufs)
	if err != nil {
		level.Error(c.logger).Log("msg", "failed to put to redis", "name", c.name, "err", err)
	}
}

// Stop stops the redis client.
func (c *RedisCache) Stop() {
	_ = c.redis.Close()
}
