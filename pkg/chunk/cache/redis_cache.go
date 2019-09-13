package cache

import (
	"context"
	"errors"
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

type redisClient struct {
	pool *redis.Pool
}

var (
	redisRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "redis_request_duration_seconds",
		Help:      "Total time spent in seconds doing redis requests.",
		Buckets:   prometheus.ExponentialBuckets(0.00025, 4, 6),
	}, []string{"method", "status_code", "name"})

	errRedisQueryTimeout = errors.New("redis query timeout")
)

// NewRedisCache creates a new RedisCache
func NewRedisCache(cfg StoreConfig, name string, client RedisClient) *RedisCache {
	// client != nil in unit tests
	if client == nil {
		client = &redisClient{
			pool: &redis.Pool{
				MaxIdle:   cfg.MaxIdleConns,
				MaxActive: cfg.MaxActiveConns,
				Dial: func() (redis.Conn, error) {
					c, err := redis.Dial("tcp", cfg.Service)
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

	if err := cache.ping(context.Background()); err != nil {
		level.Error(util.Logger).Log("msg", "error connecting to redis", "endpoint", cfg.Service, "err", err)
	}

	return cache
}

// Fetch gets keys from the cache. The keys that are found must be in the order of the keys requested.
func (c *RedisCache) Fetch(ctx context.Context, keys []string) (found []string, bufs [][]byte, missed []string) {
	var data [][]byte
	err := instr.CollectedRequest(ctx, "Redis.Fetch", c.requestDuration, redisStatusCode, func(ctx context.Context) (err error) {
		data, err = c.mget(ctx, keys)
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
	err := instr.CollectedRequest(ctx, "Redis.Store", c.requestDuration, redisStatusCode, func(ctx context.Context) error {
		return c.mset(ctx, keys, bufs, c.expiration)
	})
	if err != nil {
		level.Error(util.Logger).Log("msg", "failed to put to redis", "name", c.name, "err", err)
	}
}

// Stop stops the redis client.
func (c *RedisCache) Stop() error {
	return c.client.Close()
}

// mset adds key-value pairs to the cache.
func (c *RedisCache) mset(ctx context.Context, keys []string, bufs [][]byte, ttl int) error {
	res := make(chan error, 1)

	conn := c.client.Connection()
	defer conn.Close()

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	go func() {
		var err error
		defer func() { res <- err }()

		if err = conn.Send("MULTI"); err != nil {
			return
		}
		for i := range keys {
			if err = conn.Send("SETEX", keys[i], ttl, bufs[i]); err != nil {
				return
			}
		}
		_, err = conn.Do("EXEC")
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
func (c *RedisCache) mget(ctx context.Context, keys []string) ([][]byte, error) {
	intf := make([]interface{}, len(keys))
	for i, key := range keys {
		intf[i] = key
	}
	res := make(chan *mgetResult, 1)

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
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

func (c *RedisCache) ping(ctx context.Context) error {
	res := make(chan error, 1)

	conn := c.client.Connection()
	defer conn.Close()

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	go func() {
		pong, err := conn.Do("PING")
		if err == nil {
			_, err = redis.String(pong, err)
		}
		res <- err
	}()

	select {
	case err := <-res:
		return err
	case <-ctx.Done():
		return errRedisQueryTimeout
	}
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

// Connection returns redis Connection object.
func (c *redisClient) Connection() redis.Conn {
	return c.pool.Get()
}

// Close cleans up redis client.
func (c *redisClient) Close() error {
	return c.pool.Close()
}
