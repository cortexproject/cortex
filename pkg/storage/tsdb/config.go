package tsdb

import (
	"context"
	"errors"
	"flag"
	"time"

	"github.com/cortexproject/cortex/pkg/storage/tsdb/backend/gcs"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/backend/s3"
	"github.com/go-kit/kit/log"
	"github.com/thanos-io/thanos/pkg/objstore"
)

// Constants for the supported backends
const (
	BackendS3  = "s3"
	BackendGCS = "gcs"
)

// Config holds the config information for TSDB storage
type Config struct {
	Dir          string        `yaml:"dir"`
	SyncDir      string        `yaml:"sync_dir"`
	BlockRanges  time.Duration `yaml:"block_ranges_period"`
	Retention    time.Duration `yaml:"retention_period"`
	ShipInterval time.Duration `yaml:"ship_interval"`
	Backend      string        `yaml:"backend"`

	// Backends
	S3  s3.Config  `yaml:"s3"`
	GCS gcs.Config `yaml:"gcs"`
}

// RegisterFlags registers the TSDB flags
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.S3.RegisterFlags(f)

	f.StringVar(&cfg.Dir, "experimental.tsdb.dir", "tsdb", "directory to place all TSDB's into")
	f.StringVar(&cfg.SyncDir, "experimental.tsdb.sync-dir", "tsdb-sync", "directory to place synced tsdb indicies")
	f.DurationVar(&cfg.BlockRanges, "experimental.tsdb.block-ranges-period", 1*time.Hour, "TSDB block ranges")
	f.DurationVar(&cfg.Retention, "experimental.tsdb.retention-period", 6*time.Hour, "TSDB block retention")
	f.DurationVar(&cfg.ShipInterval, "experimental.tsdb.ship-interval", 30*time.Second, "the frequency at which tsdb blocks are scanned for shipping")
	f.StringVar(&cfg.Backend, "experimental.tsdb.backend", "s3", "TSDB storage backend to use")
}

// Validate the config
func (cfg *Config) Validate() error {
	if cfg.Backend != BackendS3 && cfg.Backend != BackendGCS {
		return errors.New("unsupported TSDB storage backend")
	}

	return nil
}

// NewBucketClient creates a new bucket client based on the configured backend
func (cfg *Config) NewBucketClient(ctx context.Context, name string, logger log.Logger) (objstore.Bucket, error) {
	switch cfg.Backend {
	case BackendS3:
		return cfg.S3.NewBucketClient(name, logger)
	case BackendGCS:
		return cfg.GCS.NewBucketClient(ctx, name, logger)
	default:
		return nil, errors.New("unsupported TSDB storage backend")
	}
}
