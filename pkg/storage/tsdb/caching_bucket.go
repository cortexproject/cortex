package tsdb

import (
	"flag"
	"fmt"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/cache"
	"github.com/thanos-io/thanos/pkg/cacheutil"
	"github.com/thanos-io/thanos/pkg/objstore"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
)

type ChunksCacheConfig struct {
	Backend   string                `yaml:"backend"`
	Memcached MemcachedClientConfig `yaml:"memcached"`

	// Basic unit used to cache chunks.
	SubrangeSize        int64 `yaml:"subrange_size"`
	MaxGetRangeRequests int   `yaml:"max_get_range_requests"`

	// TTLs for various cache items.
	ObjectSizeTTL time.Duration `yaml:"object_size_ttl"`
	SubrangeTTL   time.Duration `yaml:"subrange_ttl"`
}

func (cfg *ChunksCacheConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Backend, prefix+"backend", "", fmt.Sprintf("Backend for chunks cache. Supported values: %s.", storecache.MemcachedBucketCacheProvider))

	cfg.Memcached.RegisterFlagsWithPrefix(f, prefix+"memcached.")

	defaultCfg := storecache.DefaultCachingBucketConfig()
	cfg.SubrangeSize = defaultCfg.ChunkSubrangeSize
	cfg.ObjectSizeTTL = defaultCfg.ChunkObjectSizeTTL
	cfg.SubrangeTTL = defaultCfg.ChunkSubrangeTTL
	cfg.MaxGetRangeRequests = defaultCfg.MaxChunksGetRangeRequests

	f.IntVar(&cfg.MaxGetRangeRequests, prefix+"max-chunks-get-range-requests", cfg.MaxGetRangeRequests, "Maximum number of sub-GetRange requests that a single GetRange request can be split into when fetching chunks. Zero or negative value = unlimited number of sub-requests.")
	f.Int64Var(&cfg.SubrangeSize, prefix+"chunk-subrange-size", cfg.SubrangeSize, "Size of each subrange that bucket object is split into for better caching.")
	f.DurationVar(&cfg.ObjectSizeTTL, prefix+"chunk-object-size-ttl", cfg.ObjectSizeTTL, "TTL for caching object size for chunks.")
	f.DurationVar(&cfg.SubrangeTTL, prefix+"chunk-subrange-ttl", cfg.SubrangeTTL, "TTL for caching individual chunks subranges.")
}

// Validate the config.
func (cfg *ChunksCacheConfig) Validate() error {
	if cfg.Backend != "" && cfg.Backend != string(storecache.MemcachedBucketCacheProvider) {
		return fmt.Errorf("unsupported cache backend: %s", cfg.Backend)
	}

	if cfg.Backend == string(storecache.MemcachedBucketCacheProvider) {
		if err := cfg.Memcached.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func CreateCachingBucket(chunksConfig ChunksCacheConfig, bkt objstore.Bucket, logger log.Logger, reg prometheus.Registerer) (objstore.Bucket, error) {
	var chunksCache cache.Cache

	switch chunksConfig.Backend {
	case "":
		// No caching.
		return bkt, nil

	case string(storecache.MemcachedBucketCacheProvider):
		var memcached cacheutil.MemcachedClient
		memcached, err := cacheutil.NewMemcachedClientWithConfig(logger, "chunks-cache", chunksConfig.Memcached.ToMemcachedClientConfig(), reg)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create memcached client")
		}
		chunksCache = cache.NewMemcachedCache("chunks-cache", logger, memcached, reg)

	default:
		return nil, errors.Errorf("unsupported cache type: %s", chunksConfig.Backend)
	}

	cc := storecache.CachingBucketConfig{
		ChunkSubrangeSize:         chunksConfig.SubrangeSize,
		MaxChunksGetRangeRequests: chunksConfig.MaxGetRangeRequests,
		ChunkObjectSizeTTL:        chunksConfig.ObjectSizeTTL,
		ChunkSubrangeTTL:          chunksConfig.SubrangeTTL,
	}
	return storecache.NewCachingBucket(bkt, chunksCache, cc, logger, reg)
}
