package cache

import (
	"flag"
	"sync"
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

	pool *redis.Pool
	conn redis.Conn

	quit chan struct{}
	wait sync.WaitGroup
}

// RedisClientConfig defines how a RedisClient should be constructed.
type RedisClientConfig struct {
	Endpoint       string        `yaml:"endpoint,omitempty"`
	Timeout        time.Duration `yaml:"timeout,omitempty"`
	MaxIdleConns   int           `yaml:"max_idle_conns,omitempty"`
	MaxActiveConns int           `yaml:"max_active_conns,omitempty"`
	UpdateInterval time.Duration `yaml:"update_interval,omitempty"`
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet
func (cfg *RedisClientConfig) RegisterFlagsWithPrefix(prefix, description string, f *flag.FlagSet) {
	f.StringVar(&cfg.Endpoint, prefix+"redis.endpoint", "", description+"Redis service endpoint to use when caching chunks. If empty, no redis will be used.")
	f.DurationVar(&cfg.Timeout, prefix+"redis.timeout", 100*time.Millisecond, description+"Maximum time to wait before giving up on redis requests.")
	f.IntVar(&cfg.MaxIdleConns, prefix+"redis.max-idle-conns", 80, description+"Maximum number of idle connections in pool.")
	f.IntVar(&cfg.MaxActiveConns, prefix+"redis.max-active-conns", 0, description+"Maximum number of active connections in pool.")
	f.DurationVar(&cfg.UpdateInterval, prefix+"redis.update-interval", 1*time.Minute, description+"Period with which to ping redis service.")
}

// NewRedisClient creates a new RedisClient
func NewRedisClient(cfg RedisClientConfig) RedisClient {
	pool := &redis.Pool{
		MaxIdle:   cfg.MaxIdleConns,
		MaxActive: cfg.MaxActiveConns,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", cfg.Endpoint, redis.DialConnectTimeout(cfg.Timeout))
			if err != nil {
				return nil, err
			}
			return c, err
		},
	}

	client := &redisClient{
		endpoint: cfg.Endpoint,
		pool:     pool,
		conn:     pool.Get(),
	}

	if err := client.ping(); err != nil {
		level.Error(util.Logger).Log("msg", "error connecting to redis", "endpoint", cfg.Endpoint, "err", err)
	}

	client.wait.Add(1)
	go client.updateLoop(cfg.UpdateInterval)
	return client
}

// Set adds a key-value pair to the cache.
func (c *redisClient) Set(key string, buf []byte, ttl int) error {
	_, err := c.conn.Do("SETEX", key, ttl, buf)
	return err
}

// MGet retrieves values from the cache.
func (c *redisClient) MGet(keys []string) ([][]byte, error) {
	intf := make([]interface{}, len(keys))
	for i, key := range keys {
		intf[i] = key
	}
	return redis.ByteSlices(c.conn.Do("MGET", intf...))
}

// Stop stops the redis client.
func (c *redisClient) Stop() {
	close(c.quit)
	c.wait.Wait()
}

func (c *redisClient) updateLoop(updateInterval time.Duration) {
	defer c.wait.Done()
	ticker := time.NewTicker(updateInterval)
	for {
		select {
		case <-ticker.C:
			err := c.ping()
			if err != nil {
				level.Warn(util.Logger).Log("msg", "error connecting to redis", "err", err)
				c.conn = c.pool.Get()
			}
		case <-c.quit:
			ticker.Stop()
			return
		}
	}
}

func (c *redisClient) ping() error {
	pong, err := c.conn.Do("PING")
	if err != nil {
		return err
	}
	_, err = redis.String(pong, err)
	if err != nil {
		return err
	}
	return nil
}
