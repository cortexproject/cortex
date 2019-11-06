package s3

import (
	"github.com/go-kit/kit/log"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
)

// NewBucketClient creates a new S3 bucket client
func NewBucketClient(cfg Config, name string, logger log.Logger) (objstore.Bucket, error) {
	bucketConfig := s3.Config{
		Bucket:    cfg.BucketName,
		Endpoint:  cfg.Endpoint,
		AccessKey: cfg.AccessKeyID,
		SecretKey: cfg.SecretAccessKey,
		Insecure:  cfg.Insecure,
	}

	return s3.NewBucketWithConfig(logger, bucketConfig, name)
}
