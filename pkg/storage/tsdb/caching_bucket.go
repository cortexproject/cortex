package tsdb

import (
	"flag"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/cache"
	"github.com/thanos-io/thanos/pkg/cacheutil"
	"github.com/thanos-io/thanos/pkg/objstore"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
)

type CacheBackend struct {
	Backend   string                `yaml:"backend"`
	Memcached MemcachedClientConfig `yaml:"memcached"`
}

// Validate the config.
func (cfg *CacheBackend) Validate() error {
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

type ChunksCacheConfig struct {
	CacheBackend `yaml:",inline"`

	SubrangeSize        int64         `yaml:"subrange_size"`
	MaxGetRangeRequests int           `yaml:"max_get_range_requests"`
	ObjectSizeTTL       time.Duration `yaml:"object_size_ttl"`
	SubrangeTTL         time.Duration `yaml:"subrange_ttl"`
}

func (cfg *ChunksCacheConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Backend, prefix+"backend", "", fmt.Sprintf("Backend for chunks cache, if not empty. Supported values: %s.", storecache.MemcachedBucketCacheProvider))

	cfg.Memcached.RegisterFlagsWithPrefix(f, prefix+"memcached.")

	f.Int64Var(&cfg.SubrangeSize, prefix+"subrange-size", 16000, "Size of each subrange that bucket object is split into for better caching.")
	f.IntVar(&cfg.MaxGetRangeRequests, prefix+"max-get-range-requests", 3, "Maximum number of sub-GetRange requests that a single GetRange request can be split into when fetching chunks. Zero or negative value = unlimited number of sub-requests.")
	f.DurationVar(&cfg.ObjectSizeTTL, prefix+"object-size-ttl", 24*time.Hour, "TTL for caching object size for chunks.")
	f.DurationVar(&cfg.SubrangeTTL, prefix+"subrange-ttl", 24*time.Hour, "TTL for caching individual chunks subranges.")
}

func (cfg *ChunksCacheConfig) Validate() error {
	return cfg.CacheBackend.Validate()
}

type MetadataCacheConfig struct {
	CacheBackend

	TenantsListTTL         time.Duration `json:"tenants_list_ttl"`
	TenantBlocksListTTL    time.Duration `json:"tenant_blocks_list_ttl"`
	ChunksListTTL          time.Duration `json:"chunks_list_ttl"`
	MetafileExistsTTL      time.Duration `json:"metafile_exists_ttl"`
	MetafileDoesntExistTTL time.Duration `json:"metafile_doesnt_exist_ttl"`
	MetafileContentTTL     time.Duration `json:"metafile_content_ttl"`
	MetafileMaxSize        int           `json:"metafile_max_size"`
}

func (cfg *MetadataCacheConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Backend, prefix+"backend", "", fmt.Sprintf("Backend for metadata cache, if not empty. Supported values: %s.", storecache.MemcachedBucketCacheProvider))

	cfg.Memcached.RegisterFlagsWithPrefix(f, prefix+"memcached.")

	f.DurationVar(&cfg.TenantsListTTL, prefix+"tenants-list-ttl", 15*time.Minute, "How long to cache list of tenants in the bucket.")
	f.DurationVar(&cfg.TenantBlocksListTTL, prefix+"tenant-blocks-list-ttl", 15*time.Minute, "How long to cache list of blocks for each tenant.")
	f.DurationVar(&cfg.TenantBlocksListTTL, prefix+"chunks-list-ttl", 24*time.Hour, "How long to cache list of chunks for a block.")

	f.DurationVar(&cfg.MetafileExistsTTL, prefix+"metafile-exists-ttl", 2*time.Hour, "How long to cache information that block metafile exists.")
	f.DurationVar(&cfg.MetafileDoesntExistTTL, prefix+"metafile-doesnt-exist-ttl", 15*time.Minute, "How long to cache information that block metafile doesn't exist.")
	f.DurationVar(&cfg.MetafileContentTTL, prefix+"metafile-content-ttl", 24*time.Hour, "How long to cache content of the metafile.")
	f.IntVar(&cfg.MetafileMaxSize, prefix+"metafile-max-size", 1*1024*1024, "Maximum size of metafile content to cache.")
}

func (cfg *MetadataCacheConfig) Validate() error {
	return cfg.CacheBackend.Validate()
}

func CreateCachingBucket(chunksConfig ChunksCacheConfig, metadataConfig MetadataCacheConfig, bkt objstore.Bucket, logger log.Logger, reg prometheus.Registerer) (objstore.Bucket, error) {
	cfg := storecache.NewCachingBucketConfig()
	cachingConfigured := false

	chunksCache, err := createCache("chunks-cache", chunksConfig.Backend, chunksConfig.Memcached, logger, reg)
	if err != nil {
		return nil, errors.Wrapf(err, "chunks-cache")
	}
	if chunksCache != nil {
		cachingConfigured = true
		cfg.CacheGetRange("chunks", chunksCache, isTSDBChunkFile, chunksConfig.SubrangeSize, chunksConfig.ObjectSizeTTL, chunksConfig.SubrangeTTL, chunksConfig.MaxGetRangeRequests)
	}

	metadataCache, err := createCache("metadata-cache", metadataConfig.Backend, metadataConfig.Memcached, logger, reg)
	if err != nil {
		return nil, errors.Wrapf(err, "metadata-cache")
	}
	if metadataCache != nil {
		cachingConfigured = true

		cfg.CacheExists("metafile", metadataCache, isMetaFile, metadataConfig.MetafileExistsTTL, metadataConfig.MetafileDoesntExistTTL)
		cfg.CacheGet("metafile", metadataCache, isMetaFile, metadataConfig.MetafileMaxSize, metadataConfig.MetafileContentTTL, metadataConfig.MetafileExistsTTL, metadataConfig.MetafileDoesntExistTTL)

		cfg.CacheIter("tenants-iter", metadataCache, isTenantsDir, metadataConfig.TenantsListTTL, storecache.JSONIterCodec{})
		cfg.CacheIter("tenant-blocks-iter", metadataCache, isTenantBlocksDir, metadataConfig.TenantBlocksListTTL, snappyIterCodec{storecache.JSONIterCodec{}})
		cfg.CacheIter("chunks-iter", metadataCache, isChunksDir, metadataConfig.ChunksListTTL, snappyIterCodec{storecache.JSONIterCodec{}})
	}

	if !cachingConfigured {
		// No caching is configured.
		return bkt, nil
	}

	return storecache.NewCachingBucket(bkt, cfg, logger, reg)
}

func createCache(cacheName string, backend string, memcached MemcachedClientConfig, logger log.Logger, reg prometheus.Registerer) (cache.Cache, error) {
	switch backend {
	case "":
		// No caching.
		return nil, nil

	case string(storecache.MemcachedBucketCacheProvider):
		var client cacheutil.MemcachedClient
		client, err := cacheutil.NewMemcachedClientWithConfig(logger, cacheName, memcached.ToMemcachedClientConfig(), prometheus.WrapRegistererWith(prometheus.Labels{"cache": cacheName}, reg))
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create memcached client")
		}
		return cache.NewMemcachedCache(cacheName, logger, client, reg), nil

	default:
		return nil, errors.Errorf("unsupported cache type: %s", backend)
	}
}

var chunksMatcher = regexp.MustCompile(`^.*/chunks/\d+$`)

func isTSDBChunkFile(name string) bool { return chunksMatcher.MatchString(name) }

func isMetaFile(name string) bool {
	return strings.HasSuffix(name, "/"+metadata.MetaFilename) || strings.HasSuffix(name, "/"+metadata.DeletionMarkFilename)
}

func isTenantsDir(name string) bool {
	return name == ""
}

var tenantDirMatcher = regexp.MustCompile("^[^/]+/?$")

func isTenantBlocksDir(name string) bool {
	return tenantDirMatcher.MatchString(name)
}

func isChunksDir(name string) bool {
	return strings.HasSuffix(name, "/chunks")
}

type snappyIterCodec struct {
	storecache.IterCodec
}

func (i snappyIterCodec) Encode(files []string) ([]byte, error) {
	b, err := i.IterCodec.Encode(files)
	if err != nil {
		return nil, err
	}
	return snappy.Encode(nil, b), nil
}

func (i snappyIterCodec) Decode(cachedData []byte) ([]string, error) {
	b, err := snappy.Decode(nil, cachedData)
	if err != nil {
		return nil, errors.Wrap(err, "snappyIterCodec")
	}
	return i.IterCodec.Decode(b)
}
