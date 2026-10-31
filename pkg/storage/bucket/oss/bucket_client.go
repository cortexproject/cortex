package oss

import (
	"net/http"

	"github.com/go-kit/log"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/oss"
)

// NewBucketClient creates a new Alibaba Cloud OSS bucket client.
func NewBucketClient(cfg Config, hedgedRoundTripper func(rt http.RoundTripper) http.RoundTripper, name string, logger log.Logger) (objstore.Bucket, error) {
	bucketConfig := oss.Config{
		Endpoint:        cfg.Endpoint,
		Bucket:          cfg.Bucket,
		AccessKeyID:     cfg.AccessKeyID,
		AccessKeySecret: cfg.AccessKeySecret.Value,
	}

	// Check the error explicitly so a nil *oss.Bucket is not returned as a non-nil objstore.Bucket.
	bkt, err := oss.NewBucketWithConfig(logger, bucketConfig, name, hedgedRoundTripper)
	if err != nil {
		return nil, err
	}
	return bkt, nil
}
