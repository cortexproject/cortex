package gcs

import (
	"context"
	"net/http"

	"github.com/go-kit/log"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/gcs"
)

// NewBucketClient creates a new GCS bucket client
func NewBucketClient(ctx context.Context, cfg Config, hedgedRoundTripper func(rt http.RoundTripper) http.RoundTripper, name string, logger log.Logger) (objstore.Bucket, error) {
	bucketConfig := gcs.Config{
		Bucket:         cfg.BucketName,
		ServiceAccount: cfg.ServiceAccount.Value,
	}

	return gcs.NewBucketWithConfig(ctx, logger, bucketConfig, name, hedgedRoundTripper)
}
