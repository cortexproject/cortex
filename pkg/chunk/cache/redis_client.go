package cache

import (
	"context"
	"errors"
	"flag"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/gomodule/redigo/redis"
)

// RedisClient interface exists for mocking redisClient.
type RedisClient interface {
	Set(string, []byte, int) error
	MGet([]string) ([][]byte, error)
	Stop()
}

type redisClient struct {
	endpoint string
	timeout  time.Duration
	pool     *redis.Pool
}

// RedisClientConfig defines how a RedisClient should be constructed.
type RedisClientConfig struct {
	Endpoint       string        `yaml:"endpoint,omitempty"`
	Timeout        time.Duration `yaml:"timeout,omitempty"`
	MaxIdleConns   int           `yaml:"max_idle_conns,omitempty"`
	MaxActiveConns int           `yaml:"max_active_conns,omitempty"`
}

var redisQueryTimeoutError = errors.New("redis query timeout")

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet
func (cfg *RedisClientConfig) RegisterFlagsWithPrefix(prefix, description string, f *flag.FlagSet) {
	f.StringVar(&cfg.Endpoint, prefix+"redis.endpoint", "", description+"Redis service endpoint to use when caching chunks. If empty, no redis will be used.")
	f.DurationVar(&cfg.Timeout, prefix+"redis.timeout", 100*time.Millisecond, description+"Maximum time to wait before giving up on redis requests.")
	f.IntVar(&cfg.MaxIdleConns, prefix+"redis.max-idle-conns", 80, description+"Maximum number of idle connections in pool.")
	f.IntVar(&cfg.MaxActiveConns, prefix+"redis.max-active-conns", 0, description+"Maximum number of active connections in pool.")
}

// NewRedisClient creates a new RedisClient
func NewRedisClient(cfg RedisClientConfig) RedisClient {
	pool := &redis.Pool{
		MaxIdle:   cfg.MaxIdleConns,
		MaxActive: cfg.MaxActiveConns,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", cfg.Endpoint)
			if err != nil {
				return nil, err
			}
			return c, err
		},
	}

	client := &redisClient{
		endpoint: cfg.Endpoint,
		timeout:  cfg.Timeout,
		pool:     pool,
	}

	if err := client.ping(); err != nil {
		level.Error(util.Logger).Log("msg", "error connecting to redis", "endpoint", cfg.Endpoint, "err", err)
	}

	return client
}

// Set adds a key-value pair to the cache.
func (c *redisClient) Set(key string, buf []byte, ttl int) error {
	res := make(chan error, 1)

	conn := c.pool.Get()
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
		return redisQueryTimeoutError
	}
}

type mgetResult struct {
	bufs [][]byte
	err  error
}

// MGet retrieves values from the cache.
func (c *redisClient) MGet(keys []string) ([][]byte, error) {
	intf := make([]interface{}, len(keys))
	for i, key := range keys {
		intf[i] = key
	}
	res := make(chan *mgetResult, 1)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	conn := c.pool.Get()
	defer conn.Close()

	go func() {
		bufs, err := redis.ByteSlices(conn.Do("MGET", intf...))
		res <- &mgetResult{bufs: bufs, err: err}
	}()

	select {
	case dat := <-res:
		return dat.bufs, dat.err
	case <-ctx.Done():
		return nil, redisQueryTimeoutError
	}
}

// Stop stops the redis client.
func (c *redisClient) Stop() {
	c.pool.Close()
}

func (c *redisClient) ping() error {
	conn := c.pool.Get()
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
