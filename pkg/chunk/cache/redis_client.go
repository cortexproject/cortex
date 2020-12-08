package cache

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"strings"
	"time"
	"unsafe"

	"github.com/cortexproject/cortex/pkg/util/flagext"

	"github.com/go-redis/redis/v8"
)

// RedisConfig defines how a RedisCache should be constructed.
type RedisConfig struct {
	Endpoint           string         `yaml:"endpoint"`
	MasterName         string         `yaml:"master_name"`
	Timeout            time.Duration  `yaml:"timeout"`
	Expiration         time.Duration  `yaml:"expiration"`
	DB                 int            `yaml:"db"`
	PoolSize           int            `yaml:"pool_size"`
	Password           flagext.Secret `yaml:"password"`
	EnableTLS          bool           `yaml:"tls_enabled"`
	InsecureSkipVerify bool           `yaml:"tls_insecure_skip_verify"`
	IdleTimeout        time.Duration  `yaml:"idle_timeout"`
	MaxConnAge         time.Duration  `yaml:"max_connection_age"`
	KeyPrefix          string         `yaml:"key_prefix"`
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet
func (cfg *RedisConfig) RegisterFlagsWithPrefix(prefix, description string, f *flag.FlagSet) {
	f.StringVar(&cfg.Endpoint, prefix+"redis.endpoint", "", description+"Redis Server endpoint to use for caching. A comma-separated list of endpoints for Redis Cluster or Redis Sentinel. If empty, no redis will be used.")
	f.StringVar(&cfg.MasterName, prefix+"redis.master-name", "", description+"Redis Sentinel master name. An empty string for Redis Server or Redis Cluster.")
	f.DurationVar(&cfg.Timeout, prefix+"redis.timeout", 500*time.Millisecond, description+"Maximum time to wait before giving up on redis requests.")
	f.DurationVar(&cfg.Expiration, prefix+"redis.expiration", 0, description+"How long keys stay in the redis.")
	f.IntVar(&cfg.DB, prefix+"redis.db", 0, description+"Database index.")
	f.IntVar(&cfg.PoolSize, prefix+"redis.pool-size", 0, description+"Maximum number of connections in the pool.")
	f.Var(&cfg.Password, prefix+"redis.password", description+"Password to use when connecting to redis.")
	f.BoolVar(&cfg.EnableTLS, prefix+"redis.tls-enabled", false, description+"Enable connecting to redis with TLS.")
	f.BoolVar(&cfg.InsecureSkipVerify, prefix+"redis.tls-insecure-skip-verify", false, description+"Skip validating server certificate.")
	f.DurationVar(&cfg.IdleTimeout, prefix+"redis.idle-timeout", 0, description+"Close connections after remaining idle for this duration. If the value is zero, then idle connections are not closed.")
	f.DurationVar(&cfg.MaxConnAge, prefix+"redis.max-connection-age", 0, description+"Close connections older than this duration. If the value is zero, then the pool does not close connections based on age.")
	f.StringVar(&cfg.KeyPrefix, prefix+"redis.key-prefix", "", description+"Adds the given value to the beginning of the key.")
}

type RedisClient struct {
	defaultExpiration time.Duration
	timeout           time.Duration
	rdb               redis.UniversalClient
	keyPrefix         string
}

// NewRedisClient creates Redis client
func NewRedisClient(cfg *RedisConfig) *RedisClient {
	opt := &redis.UniversalOptions{
		Addrs:       strings.Split(cfg.Endpoint, ","),
		MasterName:  cfg.MasterName,
		Password:    cfg.Password.Value,
		DB:          cfg.DB,
		PoolSize:    cfg.PoolSize,
		IdleTimeout: cfg.IdleTimeout,
		MaxConnAge:  cfg.MaxConnAge,
	}
	if cfg.EnableTLS {
		opt.TLSConfig = &tls.Config{InsecureSkipVerify: cfg.InsecureSkipVerify}
	}
	return &RedisClient{
		defaultExpiration: cfg.Expiration,
		timeout:           cfg.Timeout,
		rdb:               redis.NewUniversalClient(opt),
		keyPrefix:         cfg.KeyPrefix,
	}
}

func (c *RedisClient) Ping(ctx context.Context) error {
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

// If expiration is zero, use the defaultExpiration set in the client.
func (c *RedisClient) MSet(ctx context.Context, keys []string, values [][]byte, expiration time.Duration) error {
	var cancel context.CancelFunc
	if c.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	if expiration == 0 {
		expiration = c.defaultExpiration
	}

	pipe := c.rdb.TxPipeline()
	for i := range keys {
		pipe.Set(ctx, c.keyPrefix+keys[i], values[i], expiration)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (c *RedisClient) MGet(ctx context.Context, keys []string) ([][]byte, error) {
	var cancel context.CancelFunc
	if c.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	var requestKeys []string
	if c.keyPrefix == "" {
		requestKeys = keys
	} else {
		requestKeys = make([]string, 0, len(keys))
		for _, key := range keys {
			requestKeys = append(requestKeys, c.keyPrefix+key)
		}
	}

	cmd := c.rdb.MGet(ctx, requestKeys...)
	if err := cmd.Err(); err != nil {
		return nil, err
	}

	ret := make([][]byte, len(requestKeys))
	for i, val := range cmd.Val() {
		if val != nil {
			ret[i] = StringToBytes(val.(string))
		}
	}
	return ret, nil
}

func (c *RedisClient) Close() error {
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
