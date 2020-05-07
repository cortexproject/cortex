package s3

import (
	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/s3"

	"github.com/cortexproject/cortex/pkg/storage/backend/instrumentation"
)

// NewBucketClient creates a new S3 bucket client
func NewBucketClient(cfg Config, name string, logger log.Logger, reg prometheus.Registerer) (objstore.Bucket, error) {
	client, err := s3.NewBucketWithConfig(logger, newS3Config(cfg), name)
	if err != nil {
		return nil, err
	}

	return instrumentation.BucketWithMetrics(client, name, reg), nil
}

// NewBucketReaderClient creates a new S3 bucket client
func NewBucketReaderClient(cfg Config, name string, logger log.Logger, reg prometheus.Registerer) (objstore.BucketReader, error) {
	client, err := s3.NewBucketWithConfig(logger, newS3Config(cfg), name)
	if err != nil {
		return nil, err
	}

	return instrumentation.BucketWithMetrics(client, name, reg), nil
}

func newS3Config(cfg Config) s3.Config {
	return s3.Config{
		Bucket:    cfg.BucketName,
		Endpoint:  cfg.Endpoint,
		AccessKey: cfg.AccessKeyID,
		SecretKey: cfg.SecretAccessKey.Value,
		Insecure:  cfg.Insecure,
	}
}
