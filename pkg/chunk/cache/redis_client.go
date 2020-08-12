package cache

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"
	"unsafe"

	"github.com/cortexproject/cortex/pkg/util/flagext"

	"github.com/go-redis/redis/v8"
)

const (
	redisTopologyServer   string = "server"
	redisTopologyCluster  string = "cluster"
	redisTopologySentinel string = "sentinel"
)

var ErrNoMasters = errors.New("redis: no masters")

// RedisConfig defines how a RedisCache should be constructed.
type RedisConfig struct {
	Topology    string         `yaml:"topology"`
	Endpoint    string         `yaml:"endpoint"`
	Timeout     time.Duration  `yaml:"timeout"`
	Expiration  time.Duration  `yaml:"expiration"`
	PoolSize    int            `yaml:"pool_size"`
	Password    flagext.Secret `yaml:"password"`
	EnableTLS   bool           `yaml:"enable_tls"`
	IdleTimeout time.Duration  `yaml:"idle_timeout"`
	MaxConnAge  time.Duration  `yaml:"max_connection_age"`
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet
func (cfg *RedisConfig) RegisterFlagsWithPrefix(prefix, description string, f *flag.FlagSet) {
	f.StringVar(&cfg.Topology, prefix+"redis.topology", redisTopologyServer, description+"Redis topology. Supported: "+redisTopologyServer+", "+redisTopologyCluster+", "+redisTopologySentinel+".")
	f.StringVar(&cfg.Endpoint, prefix+"redis.endpoint", "", description+"Redis service endpoint to use for caching. A comma-separated list of endpoints when the topology is cluster. If empty, no redis will be used.")
	f.DurationVar(&cfg.Timeout, prefix+"redis.timeout", 100*time.Millisecond, description+"Maximum time to wait before giving up on redis requests.")
	f.DurationVar(&cfg.Expiration, prefix+"redis.expiration", 0, description+"How long keys stay in the redis.")
	f.IntVar(&cfg.PoolSize, prefix+"redis.pool-size", 0, description+"Maximum number of connections in the pool.")
	f.Var(&cfg.Password, prefix+"redis.password", description+"Password to use when connecting to redis.")
	f.BoolVar(&cfg.EnableTLS, prefix+"redis.enable-tls", false, description+"Enables connecting to redis with TLS.")
	f.DurationVar(&cfg.IdleTimeout, prefix+"redis.idle-timeout", 0, description+"Close connections after remaining idle for this duration. If the value is zero, then idle connections are not closed.")
	f.DurationVar(&cfg.MaxConnAge, prefix+"redis.max-connection-age", 0, description+"Close connections older than this duration. If the value is zero, then the pool does not close connections based on age.")
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

// RedisClient is a generic Redis client interface
type RedisClient interface {
	Ping(context.Context) error
	MSet(context.Context, []string, [][]byte) error
	MGet(context.Context, []string) ([][]byte, error)
	Close() error
}

type redisCommander interface {
	Ping(ctx context.Context) *redis.StatusCmd
	TxPipeline() redis.Pipeliner
	MGet(ctx context.Context, keys ...string) *redis.SliceCmd
	Close() error
}

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

type redisBasicClient struct {
	expiration time.Duration
	timeout    time.Duration
	rdb        redisCommander
}

func newRedisServerClient(cfg *RedisConfig) *redisBasicClient {
	opt := &redis.Options{
		Addr:        cfg.Endpoint,
		Password:    cfg.Password.Value,
		PoolSize:    cfg.PoolSize,
		IdleTimeout: cfg.IdleTimeout,
		MaxConnAge:  cfg.MaxConnAge,
	}
	if cfg.EnableTLS {
		opt.TLSConfig = &tls.Config{}
	}
	return &redisBasicClient{
		expiration: cfg.Expiration,
		timeout:    cfg.Timeout,
		rdb:        redis.NewClient(opt),
	}
}

func newRedisClusterClient(cfg *RedisConfig) *redisBasicClient {
	opt := &redis.ClusterOptions{
		Addrs:       strings.Split(cfg.Endpoint, ","),
		Password:    cfg.Password.Value,
		PoolSize:    cfg.PoolSize,
		IdleTimeout: cfg.IdleTimeout,
		MaxConnAge:  cfg.MaxConnAge,
	}
	if cfg.EnableTLS {
		opt.TLSConfig = &tls.Config{}
	}
	return &redisBasicClient{
		expiration: cfg.Expiration,
		timeout:    cfg.Timeout,
		rdb:        redis.NewClusterClient(opt),
	}
}

func (c *redisBasicClient) Ping(ctx context.Context) error {
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

func (c *redisBasicClient) MSet(ctx context.Context, keys []string, values [][]byte) error {
	var cancel context.CancelFunc
	if c.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	pipe := c.rdb.TxPipeline()
	for i := range keys {
		pipe.Set(ctx, keys[i], values[i], c.expiration)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (c *redisBasicClient) MGet(ctx context.Context, keys []string) ([][]byte, error) {
	var cancel context.CancelFunc
	if c.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	cmd := c.rdb.MGet(ctx, keys...)
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

func (c *redisBasicClient) Close() error {
	return c.rdb.Close()
}

type redisSentinelClient struct {
	expiration time.Duration
	timeout    time.Duration
	rdb        *redis.SentinelClient
	opt        *redis.Options
}

func newRedisSentinelClient(cfg *RedisConfig) *redisSentinelClient {
	opt := &redis.Options{
		Addr:        cfg.Endpoint,
		Password:    cfg.Password.Value,
		PoolSize:    cfg.PoolSize,
		IdleTimeout: cfg.IdleTimeout,
		MaxConnAge:  cfg.MaxConnAge,
	}
	if cfg.EnableTLS {
		opt.TLSConfig = &tls.Config{}
	}
	return &redisSentinelClient{
		expiration: cfg.Expiration,
		timeout:    cfg.Timeout,
		rdb:        redis.NewSentinelClient(opt),
		opt:        opt,
	}
}

func (c *redisSentinelClient) getMaster(ctx context.Context) (*redisBasicClient, error) {
	masters, err := c.rdb.Masters(ctx).Result()
	if err != nil {
		return nil, err
	}
	err = ErrNoMasters
	if len(masters) == 0 {
		return nil, err
	}
	for _, master := range masters {
		// expected: master = []interface {}{"name", "<master name>", "ip", "<IP>", "port", "<port>", ... }
		if info, ok := master.([]interface{}); ok {
			if len(info) >= 6 && info[2] == "ip" && info[4] == "port" {
				return &redisBasicClient{
					expiration: c.expiration,
					timeout:    c.timeout,
					rdb: redis.NewClient(&redis.Options{
						Addr:        fmt.Sprintf("%s:%s", info[3], info[5]),
						Password:    c.opt.Password,
						PoolSize:    c.opt.PoolSize,
						IdleTimeout: c.opt.IdleTimeout,
						MaxConnAge:  c.opt.MaxConnAge,
					}),
				}, nil
			}
			err = fmt.Errorf("redis: unexpected master info format %v", info)
		} else {
			err = fmt.Errorf("redis: unexpected master info type %#v", master)
		}
	}
	return nil, err
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

// StringToBytes converts string to byte slice. (copied from vendor/github.com/go-redis/redis/v8/internal/util/unsafe.go)
func StringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(
		&struct {
			string
			Cap int
		}{s, len(s)},
	))
}
