package cache

import (
	"context"
	"flag"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/gomodule/redigo/redis"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	instr "github.com/weaveworks/common/instrument"
)

var (
	redisRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "redis_request_duration_seconds",
		Help:      "Total time spent in seconds doing redis requests.",
		// Redis requests are very quick: smallest bucket is 16us, biggest is 1s
		Buckets: prometheus.ExponentialBuckets(0.000016, 4, 8),
	}, []string{"method", "status_code", "name"})
)

// RedisConfig is config to make a Redis
type RedisConfig struct {
	Expiration time.Duration `yaml:"expiration,omitempty"`
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet
func (cfg *RedisConfig) RegisterFlagsWithPrefix(prefix, description string, f *flag.FlagSet) {
	f.DurationVar(&cfg.Expiration, prefix+"redis.expiration", 0, description+"How long keys stay in the redis.")
}

// Redis type caches chunks in redis
type Redis struct {
	cfg   RedisConfig
	redis RedisClient
	name  string

	expiration      int
	requestDuration observableVecCollector
}

// NewRedis makes a new Redis.
func NewRedis(cfg RedisConfig, client RedisClient, name string) *Redis {
	c := &Redis{
		cfg:        cfg,
		redis:      client,
		name:       name,
		expiration: int(cfg.Expiration.Seconds()),
		requestDuration: observableVecCollector{
			v: redisRequestDuration.MustCurryWith(prometheus.Labels{
				"name": name,
			}),
		},
	}
	return c
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
func (c *Redis) Fetch(ctx context.Context, keys []string) (found []string, bufs [][]byte, missed []string) {
	var data [][]byte
	err := instr.CollectedRequest(ctx, "Redis.Get", c.requestDuration, redisStatusCode, func(ctx context.Context) (err error) {
		data, err = c.redis.MGet(keys)
		return err
	})
	if err != nil {
		level.Error(util.Logger).Log("msg", "failed to get from redis", "name", c.name, "err", err)
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
func (c *Redis) Store(ctx context.Context, keys []string, bufs [][]byte) {
	for i := range keys {
		err := instr.CollectedRequest(ctx, "Redis.Put", c.requestDuration, redisStatusCode, func(_ context.Context) error {
			return c.redis.Set(keys[i], bufs[i], c.expiration)
		})
		if err != nil {
			level.Error(util.Logger).Log("msg", "failed to put to redis", "name", c.name, "err", err)
		}
	}
}

// Stop stops Redis client.
func (c *Redis) Stop() error {
	c.redis.Stop()
	return nil
}
