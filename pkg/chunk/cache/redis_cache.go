package cache

import (
	"context"
	"errors"
	"flag"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/gomodule/redigo/redis"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	instr "github.com/weaveworks/common/instrument"
)

// RedisClient interface exists for mocking redisClient.
type RedisClient interface {
	Connection() redis.Conn
	Close() error
}

// RedisCache type caches chunks in redis
type RedisCache struct {
	name            string
	expiration      int
	timeout         time.Duration
	client          RedisClient
	requestDuration observableVecCollector
}

// RedisConfig defines how a RedisCache should be constructed.
type RedisConfig struct {
	Endpoint       string        `yaml:"endpoint,omitempty"`
	Timeout        time.Duration `yaml:"timeout,omitempty"`
	Expiration     time.Duration `yaml:"expiration,omitempty"`
	MaxIdleConns   int           `yaml:"max_idle_conns,omitempty"`
	MaxActiveConns int           `yaml:"max_active_conns,omitempty"`
}

type redisClient struct {
	pool *redis.Pool
}

var (
	redisRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "redis_request_duration_seconds",
		Help:      "Total time spent in seconds doing redis requests.",
		// Redis requests are very quick: smallest bucket is 16us, biggest is 1s
		Buckets: prometheus.ExponentialBuckets(0.000016, 4, 8),
	}, []string{"method", "status_code", "name"})

	errRedisQueryTimeout = errors.New("redis query timeout")
)

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet
func (cfg *RedisConfig) RegisterFlagsWithPrefix(prefix, description string, f *flag.FlagSet) {
	f.StringVar(&cfg.Endpoint, prefix+"redis.endpoint", "", description+"Redis service endpoint to use when caching chunks. If empty, no redis will be used.")
	f.DurationVar(&cfg.Timeout, prefix+"redis.timeout", 100*time.Millisecond, description+"Maximum time to wait before giving up on redis requests.")
	f.DurationVar(&cfg.Expiration, prefix+"redis.expiration", 0, description+"How long keys stay in the redis.")
	f.IntVar(&cfg.MaxIdleConns, prefix+"redis.max-idle-conns", 80, description+"Maximum number of idle connections in pool.")
	f.IntVar(&cfg.MaxActiveConns, prefix+"redis.max-active-conns", 0, description+"Maximum number of active connections in pool.")
}

// NewRedisCache creates a new RedisCache
func NewRedisCache(cfg RedisConfig, name string, client RedisClient) *RedisCache {
	// client != nil in unit tests
	if client == nil {
		client = &redisClient{
			pool: &redis.Pool{
				MaxIdle:   cfg.MaxIdleConns,
				MaxActive: cfg.MaxActiveConns,
				Dial: func() (redis.Conn, error) {
					c, err := redis.Dial("tcp", cfg.Endpoint)
					if err != nil {
						return nil, err
					}
					return c, err
				},
			},
		}
	}

	cache := &RedisCache{
		expiration: int(cfg.Expiration.Seconds()),
		timeout:    cfg.Timeout,
		name:       name,
		client:     client,
		requestDuration: observableVecCollector{
			v: redisRequestDuration.MustCurryWith(prometheus.Labels{
				"name": name,
			}),
		},
	}

	if err := cache.ping(); err != nil {
		level.Error(util.Logger).Log("msg", "error connecting to redis", "endpoint", cfg.Endpoint, "err", err)
	}

	return cache
}

func redisStatusCode(err error) string {
	switch err {
	case nil:
		return "200"
	case redis.ErrNil:
		return "404"
	default:
		return "500"
	}
}

// Fetch gets keys from the cache. The keys that are found must be in the order of the keys requested.
func (c *RedisCache) Fetch(ctx context.Context, keys []string) (found []string, bufs [][]byte, missed []string) {
	var data [][]byte
	err := instr.CollectedRequest(ctx, "Redis.Get", c.requestDuration, redisStatusCode, func(ctx context.Context) (err error) {
		data, err = c.mget(keys)
		return err
	})
	if err != nil {
		level.Error(util.Logger).Log("msg", "failed to get from redis", "name", c.name, "err", err)
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
	for i := range keys {
		err := instr.CollectedRequest(ctx, "Redis.Put", c.requestDuration, redisStatusCode, func(_ context.Context) error {
			return c.set(keys[i], bufs[i], c.expiration)
		})
		if err != nil {
			level.Error(util.Logger).Log("msg", "failed to put to redis", "name", c.name, "err", err)
		}
	}
}

// Stop stops the redis client.
func (c *RedisCache) Stop() error {
	return c.client.Close()
}

// set adds a key-value pair to the cache.
func (c *RedisCache) set(key string, buf []byte, ttl int) error {
	res := make(chan error, 1)

	conn := c.client.Connection()
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	go func() {
		_, err := conn.Do("SETEX", key, ttl, buf)
		res <- err
	}()

	select {
	case err := <-res:
		return err
	case <-ctx.Done():
		return errRedisQueryTimeout
	}
}

type mgetResult struct {
	bufs [][]byte
	err  error
}

// mget retrieves values from the cache.
func (c *RedisCache) mget(keys []string) ([][]byte, error) {
	intf := make([]interface{}, len(keys))
	for i, key := range keys {
		intf[i] = key
	}
	res := make(chan *mgetResult, 1)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	conn := c.client.Connection()
	defer conn.Close()

	go func() {
		bufs, err := redis.ByteSlices(conn.Do("MGET", intf...))
		res <- &mgetResult{bufs: bufs, err: err}
	}()

	select {
	case dat := <-res:
		return dat.bufs, dat.err
	case <-ctx.Done():
		return nil, errRedisQueryTimeout
	}
}

func (c *RedisCache) ping() error {
	conn := c.client.Connection()
	defer conn.Close()

	pong, err := conn.Do("PING")
	if err != nil {
		return err
	}
	_, err = redis.String(pong, err)
	if err != nil {
		return err
	}
	return nil
}

// Connection returns redis Connection object.
func (c *redisClient) Connection() redis.Conn {
	return c.pool.Get()
}

// Close cleans up redis client.
func (c *redisClient) Close() error {
	return c.pool.Close()
}
