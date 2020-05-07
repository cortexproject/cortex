package tsdb

import (
	"context"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/storage/backend/azure"
	"github.com/cortexproject/cortex/pkg/storage/backend/filesystem"
	"github.com/cortexproject/cortex/pkg/storage/backend/gcs"
	"github.com/cortexproject/cortex/pkg/storage/backend/s3"
)

// NewBucketClient creates a new bucket client based on the configured backend
func NewBucketClient(ctx context.Context, cfg Config, name string, logger log.Logger, reg prometheus.Registerer) (objstore.Bucket, error) {
	switch cfg.Backend {
	case BackendS3:
		return s3.NewBucketClient(cfg.S3, name, logger, reg)
	case BackendGCS:
		return gcs.NewBucketClient(ctx, cfg.GCS, name, logger, reg)
	case BackendAzure:
		return azure.NewBucketClient(cfg.Azure, name, logger, reg)
	case BackendFilesystem:
		return filesystem.NewBucketClient(cfg.Filesystem, name, reg)
	default:
		return nil, errUnsupportedStorageBackend
	}
}
