package cache

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"
	"unsafe"

	"github.com/go-redis/redis/v8"
)

const (
	redisTopologyServer   string = "server"
	redisTopologyCluster  string = "cluster"
	redisTopologySentinel string = "sentinel"
)

// RedisClient is a generic Redis client interface
type RedisClient interface {
	Ping(context.Context) error
	MSet(context.Context, []string, [][]byte) error
	MGet(context.Context, []string) ([][]byte, error)
	Close() error
}

type redisCmd interface {
	Ping(ctx context.Context) *redis.StatusCmd
	TxPipeline() redis.Pipeliner
	MGet(ctx context.Context, keys ...string) *redis.SliceCmd
}

type redisBasicClient struct {
	expiration time.Duration
	timeout    time.Duration
}

var (
	_ RedisClient = (*redisServerClient)(nil)
	_ RedisClient = (*redisSentinelClient)(nil)
	_ RedisClient = (*redisClusterClient)(nil)

	ErrNoMasters = errors.New("redis: no masters")
)

// NewRedisClient creates Redis client
func NewRedisClient(cfg *RedisConfig) RedisClient {
	switch cfg.Topology {
	case redisTopologyCluster:
		return newRedisClusterClient(cfg)
	case redisTopologySentinel:
		return newRedisSentinelClient(cfg)
	default:
		return newRedisServerClient(cfg)
	}
}

func ping(ctx context.Context, rdb redisCmd, c *redisBasicClient) error {
	var cancel context.CancelFunc
	if c.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		return err
	}
	if pong != "PONG" {
		return fmt.Errorf("redis: Unexpected PING response %q", pong)
	}
	return nil
}

func mset(ctx context.Context, rdb redisCmd, keys []string, values [][]byte, c *redisBasicClient) error {
	var cancel context.CancelFunc
	if c.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	pipe := rdb.TxPipeline()
	for i := range keys {
		pipe.Set(ctx, keys[i], values[i], c.expiration)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func mget(ctx context.Context, rdb redisCmd, keys []string, c *redisBasicClient) ([][]byte, error) {
	var cancel context.CancelFunc
	if c.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	cmd := rdb.MGet(ctx, keys...)
	if err := cmd.Err(); err != nil {
		return nil, err
	}

	ret := make([][]byte, len(keys))
	for i, val := range cmd.Val() {
		if val != nil {
			ret[i] = StringToBytes(val.(string))
		}
	}
	return ret, nil
}

// StringToBytes converts string to byte slice. (copied from vendor/github.com/go-redis/redis/v8/internal/util/unsafe.go)
func StringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(
		&struct {
			string
			Cap int
		}{s, len(s)},
	))
}

type redisServerClient struct {
	redisBasicClient
	rdb *redis.Client
}

func newRedisServerClient(cfg *RedisConfig) *redisServerClient {
	opt := &redis.Options{
		Addr:        cfg.Endpoint,
		Password:    cfg.Password.Value,
		PoolSize:    cfg.MaxActiveConns,
		IdleTimeout: cfg.IdleTimeout,
		MaxConnAge:  cfg.MaxConnLifetime,
	}
	if cfg.EnableTLS {
		opt.TLSConfig = &tls.Config{}
	}
	return &redisServerClient{
		redisBasicClient: redisBasicClient{
			expiration: cfg.Expiration,
			timeout:    cfg.Timeout,
		},
		rdb: redis.NewClient(opt),
	}
}

func (c *redisServerClient) Ping(ctx context.Context) error {
	return ping(ctx, c.rdb, &c.redisBasicClient)
}

func (c *redisServerClient) MSet(ctx context.Context, keys []string, values [][]byte) error {
	return mset(ctx, c.rdb, keys, values, &c.redisBasicClient)
}

func (c *redisServerClient) MGet(ctx context.Context, keys []string) ([][]byte, error) {
	return mget(ctx, c.rdb, keys, &c.redisBasicClient)
}

func (c *redisServerClient) Close() error {
	return c.rdb.Close()
}

type redisClusterClient struct {
	redisBasicClient
	rdb *redis.ClusterClient
}

func newRedisClusterClient(cfg *RedisConfig) *redisClusterClient {
	opt := &redis.ClusterOptions{
		Addrs:       strings.Split(cfg.Endpoint, ","),
		Password:    cfg.Password.Value,
		PoolSize:    cfg.MaxActiveConns,
		IdleTimeout: cfg.IdleTimeout,
		MaxConnAge:  cfg.MaxConnLifetime,
	}
	if cfg.EnableTLS {
		opt.TLSConfig = &tls.Config{}
	}
	return &redisClusterClient{
		redisBasicClient: redisBasicClient{
			expiration: cfg.Expiration,
			timeout:    cfg.Timeout,
		},
		rdb: redis.NewClusterClient(opt),
	}
}

func (c *redisClusterClient) Ping(ctx context.Context) error {
	return ping(ctx, c.rdb, &c.redisBasicClient)
}

func (c *redisClusterClient) MSet(ctx context.Context, keys []string, values [][]byte) error {
	return mset(ctx, c.rdb, keys, values, &c.redisBasicClient)
}

func (c *redisClusterClient) MGet(ctx context.Context, keys []string) ([][]byte, error) {
	return mget(ctx, c.rdb, keys, &c.redisBasicClient)
}

func (c *redisClusterClient) Close() error {
	return c.rdb.Close()
}

type redisSentinelClient struct {
	redisBasicClient
	rdb *redis.SentinelClient
	opt *redis.Options
}

func newRedisSentinelClient(cfg *RedisConfig) *redisSentinelClient {
	opt := &redis.Options{
		Addr:        cfg.Endpoint,
		Password:    cfg.Password.Value,
		PoolSize:    cfg.MaxActiveConns,
		IdleTimeout: cfg.IdleTimeout,
		MaxConnAge:  cfg.MaxConnLifetime,
	}
	if cfg.EnableTLS {
		opt.TLSConfig = &tls.Config{}
	}
	return &redisSentinelClient{
		redisBasicClient: redisBasicClient{
			expiration: cfg.Expiration,
			timeout:    cfg.Timeout,
		},
		rdb: redis.NewSentinelClient(opt),
		opt: opt,
	}
}

func (c *redisSentinelClient) getMaster(ctx context.Context) (*redisServerClient, error) {
	masters, err := c.rdb.Masters(ctx).Result()
	if err != nil {
		return nil, err
	}
	if len(masters) == 0 {
		return nil, ErrNoMasters
	}
	for _, master := range masters {
		info := master.([]interface{})
		if len(info) >= 6 && info[2] == "ip" && info[4] == "port" {
			return &redisServerClient{
				redisBasicClient: c.redisBasicClient,
				rdb: redis.NewClient(&redis.Options{
					Addr:        fmt.Sprintf("%s:%s", info[3], info[5]),
					Password:    c.opt.Password,
					PoolSize:    c.opt.PoolSize,
					IdleTimeout: c.opt.IdleTimeout,
					MaxConnAge:  c.opt.MaxConnAge,
				}),
			}, nil
		}
	}
	return nil, ErrNoMasters
}

func (c *redisSentinelClient) Ping(ctx context.Context) error {
	var cancel context.CancelFunc
	if c.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	pong, err := c.rdb.Ping(ctx).Result()
	if err != nil {
		return err
	}
	if pong != "PONG" {
		return fmt.Errorf("redis: Unexpected PING response %q", pong)
	}
	return nil
}

func (c *redisSentinelClient) MSet(ctx context.Context, keys []string, values [][]byte) error {
	master, err := c.getMaster(ctx)
	if err != nil {
		return err
	}
	return master.MSet(ctx, keys, values)
}

func (c *redisSentinelClient) MGet(ctx context.Context, keys []string) ([][]byte, error) {
	master, err := c.getMaster(ctx)
	if err != nil {
		return nil, err
	}
	return master.MGet(ctx, keys)
}

func (c *redisSentinelClient) Close() error {
	return c.rdb.Close()
}
