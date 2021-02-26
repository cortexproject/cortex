package alertstore

import (
	"context"
	"flag"
	"fmt"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/alertmanager/alertstore/bucketclient"
	"github.com/cortexproject/cortex/pkg/alertmanager/alertstore/configdb"
	"github.com/cortexproject/cortex/pkg/alertmanager/alertstore/local"
	"github.com/cortexproject/cortex/pkg/alertmanager/alertstore/objectclient"
	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/aws"
	"github.com/cortexproject/cortex/pkg/chunk/azure"
	"github.com/cortexproject/cortex/pkg/chunk/gcp"
	"github.com/cortexproject/cortex/pkg/configs/client"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
)

const (
	ConfigDB = "configdb"
)

// LegacyConfig configures the alertmanager storage backend using the legacy storage clients.
type LegacyConfig struct {
	Type     string        `yaml:"type"`
	ConfigDB client.Config `yaml:"configdb"`

	// Object Storage Configs
	Azure azure.BlobStorageConfig `yaml:"azure"`
	GCS   gcp.GCSConfig           `yaml:"gcs"`
	S3    aws.S3Config            `yaml:"s3"`
	Local local.StoreConfig       `yaml:"local"`
}

// RegisterFlags registers flags.
func (cfg *LegacyConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.ConfigDB.RegisterFlagsWithPrefix("alertmanager.", f)
	f.StringVar(&cfg.Type, "alertmanager.storage.type", ConfigDB, "Type of backend to use to store alertmanager configs. Supported values are: \"configdb\", \"gcs\", \"s3\", \"local\".")

	cfg.Azure.RegisterFlagsWithPrefix("alertmanager.storage.", f)
	cfg.GCS.RegisterFlagsWithPrefix("alertmanager.storage.", f)
	cfg.S3.RegisterFlagsWithPrefix("alertmanager.storage.", f)
	cfg.Local.RegisterFlags(f)
}

// Validate config and returns error on failure
func (cfg *LegacyConfig) Validate() error {
	if err := cfg.Azure.Validate(); err != nil {
		return errors.Wrap(err, "invalid Azure Storage config")
	}
	if err := cfg.S3.Validate(); err != nil {
		return errors.Wrap(err, "invalid S3 Storage config")
	}
	return nil
}

// IsDefaults returns true if the storage options have not been set.
func (cfg *LegacyConfig) IsDefaults() bool {
	return cfg.Type == ConfigDB && cfg.ConfigDB.ConfigsAPIURL.URL == nil
}

// NewLegacyAlertStore returns a new alertmanager storage backend poller and store
func NewLegacyAlertStore(cfg LegacyConfig, logger log.Logger) (AlertStore, error) {
	if cfg.Type == "configdb" {
		c, err := client.New(cfg.ConfigDB)
		if err != nil {
			return nil, err
		}
		return configdb.NewStore(c), nil
	}

	if cfg.Type == "local" {
		return local.NewStore(cfg.Local)
	}

	// Create the object store client.
	var client chunk.ObjectClient
	var err error
	switch cfg.Type {
	case "azure":
		client, err = azure.NewBlobStorage(&cfg.Azure)
	case "gcs":
		client, err = gcp.NewGCSObjectClient(context.Background(), cfg.GCS)
	case "s3":
		client, err = aws.NewS3ObjectClient(cfg.S3)
	default:
		return nil, fmt.Errorf("unrecognized alertmanager storage backend %v, choose one of: azure, configdb, gcs, local, s3", cfg.Type)
	}
	if err != nil {
		return nil, err
	}

	return objectclient.NewAlertStore(client, logger), nil
}

// Config configures a the alertmanager storage backend.
type Config struct {
	bucket.Config `yaml:",inline"`
	ConfigDB      client.Config `yaml:"configdb"`
}

// RegisterFlags registers the backend storage config.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	prefix := "alertmanager-storage."

	cfg.ExtraBackends = []string{ConfigDB}
	cfg.ConfigDB.RegisterFlagsWithPrefix(prefix, f)
	cfg.RegisterFlagsWithPrefix(prefix, f)
}

// NewAlertStore returns a alertmanager store backend client based on the provided cfg.
func NewAlertStore(ctx context.Context, cfg Config, cfgProvider bucket.TenantConfigProvider, logger log.Logger, reg prometheus.Registerer) (AlertStore, error) {
	if cfg.Backend == ConfigDB {
		c, err := client.New(cfg.ConfigDB)
		if err != nil {
			return nil, err
		}
		return configdb.NewStore(c), nil
	}

	bucketClient, err := bucket.NewClient(ctx, cfg.Config, "alertmanager-storage", logger, reg)
	if err != nil {
		return nil, err
	}

	store := bucketclient.NewBucketAlertStore(bucketClient, cfgProvider, logger)
	if err != nil {
		return nil, err
	}

	return store, nil
}
