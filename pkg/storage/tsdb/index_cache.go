package tsdb

import (
	"github.com/alecthomas/units"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
)

const (
	defaultMaxItemSize = storecache.Bytes(128 * units.MiB)
)

// NewIndexCache creates a new index cache based on the input configuration.
func NewIndexCache(cfg BucketStoreConfig, logger log.Logger, registerer prometheus.Registerer) (storecache.IndexCache, error) {
	maxCacheSize := storecache.Bytes(cfg.IndexCacheSizeBytes)

	// Calculate the max item size.
	maxItemSize := defaultMaxItemSize
	if maxItemSize > maxCacheSize {
		maxItemSize = maxCacheSize
	}

	return storecache.NewInMemoryIndexCacheWithConfig(logger, registerer, storecache.InMemoryIndexCacheConfig{
		MaxSize:     maxCacheSize,
		MaxItemSize: maxItemSize,
	})
}
