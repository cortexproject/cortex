package querier

import (
	"context"
	"net/http"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/extprom"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
)

func createCachingBucketClient(ctx context.Context, storageCfg cortex_tsdb.BlocksStorageConfig, hedgedRoundTripper func(rt http.RoundTripper) http.RoundTripper, name string, logger log.Logger, reg prometheus.Registerer) (objstore.InstrumentedBucket, error) {
	bucketClient, err := bucket.NewClient(ctx, storageCfg.Bucket, hedgedRoundTripper, name, logger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create bucket client")
	}

	// Blocks finder doesn't use chunks, but we pass config for consistency.
	matchers := cortex_tsdb.NewMatchers()
	cachingBucket, err := cortex_tsdb.CreateCachingBucket(storageCfg.BucketStore.ChunksCache, storageCfg.BucketStore.MetadataCache, storageCfg.BucketStore.ParquetLabelsCache, matchers, bucketClient, logger, extprom.WrapRegistererWith(prometheus.Labels{"component": name}, reg))
	if err != nil {
		return nil, errors.Wrap(err, "create caching bucket")
	}
	bucketClient = cachingBucket
	return bucketClient, nil
}
