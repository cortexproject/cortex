package storage

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/chunk/aws"
	"github.com/weaveworks/cortex/pkg/chunk/cache"
	"github.com/weaveworks/cortex/pkg/chunk/cassandra"
	"github.com/weaveworks/cortex/pkg/chunk/gcp"
	"github.com/weaveworks/cortex/pkg/util"
)

// Config chooses which storage client to use.
type Config struct {
	StorageClient          string
	AWSStorageConfig       aws.StorageConfig
	GCPStorageConfig       gcp.Config
	CassandraStorageConfig cassandra.Config

	IndexCacheSize     int
	IndexCacheValidity time.Duration
	memcacheClient     cache.MemcachedClientConfig
}

// RegisterFlags adds the flags required to configure this flag set.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	flag.StringVar(&cfg.StorageClient, "chunk.storage-client", "aws", "Which storage client to use (aws, gcp, cassandra, inmemory).")
	cfg.AWSStorageConfig.RegisterFlags(f)
	cfg.GCPStorageConfig.RegisterFlags(f)
	cfg.CassandraStorageConfig.RegisterFlags(f)

	f.IntVar(&cfg.IndexCacheSize, "store.index-cache-size", 0, "Size of in-memory index cache, 0 to disable.")
	f.DurationVar(&cfg.IndexCacheValidity, "store.index-cache-validity", 5*time.Minute, "Period for which entries in the index cache are valid. Should be no higher than -ingester.max-chunk-idle.")
	f.StringVar(&cfg.memcacheClient.Host, "index.memcached.hostname", "", "Hostname for memcached service to use when caching chunks. If empty, no memcached will be used.")
	f.StringVar(&cfg.memcacheClient.Service, "index.memcached.service", "memcached", "SRV service used to discover memcache servers.")
	f.DurationVar(&cfg.memcacheClient.Timeout, "index.memcached.timeout", 100*time.Millisecond, "Maximum time to wait before giving up on memcached requests.")
	f.DurationVar(&cfg.memcacheClient.UpdateInterval, "index.memcached.update-interval", 1*time.Minute, "Period with which to poll DNS for memcache servers.")
}

// Opts makes the storage clients based on the configuration.
func Opts(cfg Config, schemaCfg chunk.SchemaConfig) ([]chunk.StorageOpt, error) {
	var caches []cache.Cache
	if cfg.IndexCacheSize > 0 {
		caches = append(caches, cache.Instrument(
			"fifo-index",
			cache.NewFifoCache("index", cfg.IndexCacheSize, cfg.IndexCacheValidity),
		))
	}

	if cfg.memcacheClient.Host != "" {
		client := cache.NewMemcachedClient(cfg.memcacheClient)
		memcache := cache.NewMemcached(cache.MemcachedConfig{
			Expiration: cfg.IndexCacheValidity,
		}, client)
		caches = append(caches, cache.Instrument("memcache-index", memcache))
	}

	var tieredCache cache.Cache
	if len(caches) > 0 {
		tieredCache = cache.NewTiered(caches)
	}

	client, err := newStorageClient(cfg, schemaCfg, tieredCache)
	if err != nil {
		return nil, errors.Wrap(err, "error creating storage client")
	}

	opts := []chunk.StorageOpt{}
	opts = append(opts, chunk.StorageOpt{From: model.Time(0), Client: client})
	if cfg.StorageClient == "gcp" && schemaCfg.BigtableColumnKeyFrom.IsSet() {
		client, err = gcp.NewStorageClientColumnKey(context.Background(), cfg.GCPStorageConfig, schemaCfg)
		if err != nil {
			return nil, errors.Wrap(err, "error creating storage client")
		}

		opts = append(opts, chunk.StorageOpt{
			From:   schemaCfg.BigtableColumnKeyFrom.Time,
			Client: newCachingStorageClient(client, tieredCache, cfg.IndexCacheValidity),
		})
	}

	return opts, nil
}

func newStorageClient(cfg Config, schemaCfg chunk.SchemaConfig, cache cache.Cache) (client chunk.StorageClient, err error) {
	switch cfg.StorageClient {
	case "inmemory":
		client, err = chunk.NewMockStorage(), nil
	case "aws":
		if cfg.AWSStorageConfig.DynamoDB.URL == nil {
			return nil, fmt.Errorf("Must set -dynamodb.url in aws mode")
		}
		path := strings.TrimPrefix(cfg.AWSStorageConfig.DynamoDB.URL.Path, "/")
		if len(path) > 0 {
			level.Warn(util.Logger).Log("msg", "ignoring DynamoDB URL path", "path", path)
		}
		client, err = aws.NewStorageClient(cfg.AWSStorageConfig, schemaCfg)
	case "gcp":
		client, err = gcp.NewStorageClient(context.Background(), cfg.GCPStorageConfig, schemaCfg)
	case "cassandra":
		client, err = cassandra.NewStorageClient(cfg.CassandraStorageConfig, schemaCfg)
	default:
		return nil, fmt.Errorf("Unrecognized storage client %v, choose one of: aws, gcp, cassandra, inmemory", cfg.StorageClient)
	}

	client = newCachingStorageClient(client, cache, cfg.IndexCacheValidity)
	return
}

// NewTableClient makes a new table client based on the configuration.
func NewTableClient(cfg Config) (chunk.TableClient, error) {
	switch cfg.StorageClient {
	case "inmemory":
		return chunk.NewMockStorage(), nil
	case "aws":
		path := strings.TrimPrefix(cfg.AWSStorageConfig.DynamoDB.URL.Path, "/")
		if len(path) > 0 {
			level.Warn(util.Logger).Log("msg", "ignoring DynamoDB URL path", "path", path)
		}
		return aws.NewDynamoDBTableClient(cfg.AWSStorageConfig.DynamoDBConfig)
	case "gcp":
		return gcp.NewTableClient(context.Background(), cfg.GCPStorageConfig)
	case "cassandra":
		return cassandra.NewTableClient(context.Background(), cfg.CassandraStorageConfig)
	default:
		return nil, fmt.Errorf("Unrecognized storage client %v, choose one of: aws, gcp, inmemory", cfg.StorageClient)
	}
}
