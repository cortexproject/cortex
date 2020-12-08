package tsdb

import (
	"context"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/thanos-io/thanos/pkg/cache"

	chunkCache "github.com/cortexproject/cortex/pkg/chunk/cache"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
)

// impl thanos cache interface
var _ cache.Cache = &RedisCache{}

type RedisCache struct {
	redisClient *chunkCache.RedisClient
	logger      log.Logger
	name        string
}

// NewRedisCache creates a new RedisCache
func NewRedisCache(name string, redisClient *chunkCache.RedisClient, logger log.Logger) *RedisCache {
	util_log.WarnExperimentalUse("Redis cache")
	cache := &RedisCache{
		name:        name,
		redisClient: redisClient,
		logger:      logger,
	}
	if err := cache.redisClient.Ping(context.Background()); err != nil {
		level.Error(logger).Log("msg", "error connecting to redis", "name", name, "err", err)
	}
	return cache
}

func (r *RedisCache) Store(ctx context.Context, data map[string][]byte, ttl time.Duration) {
	keys := make([]string, 0, len(data))
	values := make([][]byte, 0, len(data))
	for key, value := range data {
		keys = append(keys, key)
		values = append(values, value)
	}
	err := r.redisClient.MSet(ctx, keys, values, ttl)
	if err != nil {
		level.Error(r.logger).Log("msg", "failed to put to redis", "name", r.name, "err", err)
	}
}

func (r *RedisCache) Fetch(ctx context.Context, keys []string) map[string][]byte {
	response, err := r.redisClient.MGet(ctx, keys)
	if err != nil {
		level.Error(r.logger).Log("msg", "failed to get from redis", "name", r.name, "err", err)
	}

	result := make(map[string][]byte)
	for index, value := range response {
		if len(value) != 0 {
			key := keys[index]
			result[key] = value
		}
	}

	return result
}
