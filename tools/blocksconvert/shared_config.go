package blocksconvert

import (
	"context"
	"flag"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
)

type SharedConfig struct {
	SchemaConfig  chunk.SchemaConfig // Flags registered by main.go
	StorageConfig storage.Config

	Bucket       tsdb.BucketConfig
	BucketPrefix string
}

func (cfg *SharedConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.SchemaConfig.RegisterFlags(f)
	cfg.Bucket.RegisterFlags(f)
	cfg.StorageConfig.RegisterFlags(f)

	f.StringVar(&cfg.BucketPrefix, "blocksconvert.bucket-prefix", "migration", "Prefix in the bucket for storing plan files.")
}

func (cfg *SharedConfig) GetBucket(l log.Logger, reg prometheus.Registerer) (objstore.Bucket, error) {
	if err := cfg.Bucket.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid bucket config")
	}

	bucket, err := tsdb.NewBucketClient(context.Background(), cfg.Bucket, "bucket", l, reg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create bucket")
	}

	return bucket, nil
}
