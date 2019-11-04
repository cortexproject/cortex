package tsdb

import (
	"context"

	"github.com/cortexproject/cortex/pkg/storage/tsdb/backend/gcs"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/backend/s3"
	"github.com/go-kit/kit/log"
	"github.com/thanos-io/thanos/pkg/objstore"
)

// NewBucketClient creates a new bucket client based on the configured backend
func NewBucketClient(ctx context.Context, cfg Config, name string, logger log.Logger) (objstore.Bucket, error) {
	switch cfg.Backend {
	case BackendS3:
		return s3.NewBucketClient(cfg.S3, name, logger)
	case BackendGCS:
		return gcs.NewBucketClient(ctx, cfg.GCS, name, logger)
	default:
		return nil, errUnsupportedBackend
	}
}
