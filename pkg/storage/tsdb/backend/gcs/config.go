package gcs

import (
	"context"
	"flag"

	"github.com/go-kit/kit/log"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/gcs"
	yaml "gopkg.in/yaml.v2"
)

// Config holds the config options for GCS backend
type Config struct {
	BucketName     string `yaml:"bucket_name"`
	ServiceAccount string `yaml:"service_account"`
}

// RegisterFlags registers the flags for TSDB GCS storage
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.BucketName, "experimental.tsdb.gcs.bucket-name", "", "GCS bucket name")
	f.StringVar(&cfg.ServiceAccount, "experimental.tsdb.gcs.service-account", "", "JSON representing either a Google Developers Console client_credentials.json file or a Google Developers service account key file. If empty, fallback to Google default logic.")
}

// NewBucketClient creates a new GCS bucket client
func (cfg *Config) NewBucketClient(ctx context.Context, name string, logger log.Logger) (objstore.Bucket, error) {
	bucketConfig := gcs.Config{
		Bucket:         cfg.BucketName,
		ServiceAccount: cfg.ServiceAccount,
	}

	// Thanos currently doesn't support passing the config as is, but expects a YAML,
	// so we're going to serialize it.
	serialized, err := yaml.Marshal(bucketConfig)
	if err != nil {
		return nil, err
	}

	return gcs.NewBucket(ctx, logger, serialized, name)
}
