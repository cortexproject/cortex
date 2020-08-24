package blocksconvert

import (
	"context"
	"flag"
	"fmt"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
)

type SharedConfig struct {
	SchemaConfig chunk.SchemaConfig // Flags registered by main.go
	Bucket       tsdb.BucketConfig
	BucketPrefix string
}

func (cfg *SharedConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.SchemaConfig.RegisterFlags(flag.CommandLine)
	cfg.Bucket.RegisterFlags(flag.CommandLine)

	f.StringVar(&cfg.BucketPrefix, "blocksconvert.bucket-prefix", "migration", "Prefix in the bucket for storing plan files.")
}

func (cfg *SharedConfig) GetBucket(l log.Logger, reg prometheus.Registerer) (objstore.Bucket, error) {
	if err := cfg.Bucket.Validate(); err != nil {
		return nil, fmt.Errorf("invalid bucket config: %w", err)
	}

	bucket, err := tsdb.NewBucketClient(context.Background(), cfg.Bucket, "bucket", l, reg)
	if err != nil {
		return nil, fmt.Errorf("failed to create bucket: %w", err)
	}

	return bucket, nil
}
