package s3

import (
	"flag"

	"github.com/go-kit/kit/log"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
)

// Config holds the config options for an S3 backend
type Config struct {
	Endpoint        string `yaml:"endpoint"`
	BucketName      string `yaml:"bucket_name"`
	SecretAccessKey string `yaml:"secret_access_key"`
	AccessKeyID     string `yaml:"access_key_id"`
	Insecure        bool   `yaml:"insecure"`
}

// RegisterFlags registers the flags for TSDB s3 storage
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.AccessKeyID, "experimental.tsdb.s3.access-key-id", "", "S3 access key ID")
	f.StringVar(&cfg.SecretAccessKey, "experimental.tsdb.s3.secret-access-key", "", "S3 secret access key")
	f.StringVar(&cfg.BucketName, "experimental.tsdb.s3.bucket-name", "", "S3 bucket name")
	f.StringVar(&cfg.Endpoint, "experimental.tsdb.s3.endpoint", "", "S3 endpoint without schema")
	f.BoolVar(&cfg.Insecure, "experimental.tsdb.s3.insecure", false, "If enabled, use http:// for the S3 endpoint instead of https://")
}

// NewBucketClient creates a new S3 bucket client
func (cfg *Config) NewBucketClient(name string, logger log.Logger) (objstore.Bucket, error) {
	bucketConfig := s3.Config{
		Bucket:    cfg.BucketName,
		Endpoint:  cfg.Endpoint,
		AccessKey: cfg.AccessKeyID,
		SecretKey: cfg.SecretAccessKey,
		Insecure:  cfg.Insecure,
	}

	return s3.NewBucketWithConfig(logger, bucketConfig, name)
}
