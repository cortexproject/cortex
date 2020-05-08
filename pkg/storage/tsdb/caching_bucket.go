package tsdb

import (
	"flag"
	"fmt"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/cache"
	"github.com/thanos-io/thanos/pkg/cacheutil"
	"github.com/thanos-io/thanos/pkg/objstore"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
)

type CachingBucketConfig struct {
	Backend   string                    `yaml:"backend"`
	Memcached MemcachedIndexCacheConfig `yaml:"memcached"`

	CachingConfig storecache.CachingBucketConfig `yaml:"caching"`
}

func (cfg *CachingBucketConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix(f, "experimental.tsdb.bucket-store.index-cache.")
}

func (cfg *CachingBucketConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Backend, prefix+"backend", "", fmt.Sprintf("Caching bucket backend type. Supported values: %s.", storecache.MemcachedBucketCacheProvider))

	cfg.Memcached.RegisterFlagsWithPrefix(f, prefix+"memcached.")
	cfg.CachingConfig = storecache.DefaultCachingBucketConfig()
	f.IntVar(&cfg.CachingConfig.MaxChunksGetRangeRequests, prefix+"max-chunks-get-range-requests", cfg.CachingConfig.MaxChunksGetRangeRequests, "Max GetRange requests for chunks.")
	f.Int64Var(&cfg.CachingConfig.ChunkSubrangeSize, prefix+"chunk-subrange-size", cfg.CachingConfig.ChunkSubrangeSize, "Subrange size used for caching chunks.")
	f.DurationVar(&cfg.CachingConfig.ChunkObjectSizeTTL, prefix+"chunk-object-size-ttl", cfg.CachingConfig.ChunkObjectSizeTTL, "TTL for caching object size for chunks.")
	f.DurationVar(&cfg.CachingConfig.ChunkSubrangeTTL, prefix+"chunk-subrange-ttl", cfg.CachingConfig.ChunkSubrangeTTL, "TTL for caching individual chunks subranges.")
}

// Validate the config.
func (cfg *CachingBucketConfig) Validate() error {
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

func CreateCachingBucket(config CachingBucketConfig, bkt objstore.Bucket, logger log.Logger, reg prometheus.Registerer) (objstore.Bucket, error) {
	var c cache.Cache

	switch config.Backend {
	case "":
		// No caching
		return bkt, nil

	case string(storecache.MemcachedBucketCacheProvider):
		var memcached cacheutil.MemcachedClient
		memcached, err := cacheutil.NewMemcachedClientWithConfig(logger, "bucket-cache", config.Memcached.ToMemcachedClientConfig(), reg)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create memcached client")
		}
		c = cache.NewMemcachedCache("bucket-cache", logger, memcached, reg)
	default:
		return nil, errors.Errorf("unsupported cache type: %s", config.Backend)
	}

	return storecache.NewCachingBucket(bkt, c, config.CachingConfig, logger, reg)
}
