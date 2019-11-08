package tsdb

import (
	"errors"
	"flag"
	"time"

	"github.com/cortexproject/cortex/pkg/storage/tsdb/backend/gcs"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/backend/s3"
)

// Constants for the config values
const (
	BackendS3      = "s3"
	BackendGCS     = "gcs"
	BackendMultiS3 = "multiS3"
)

// Validation errors
var (
	errUnsupportedBackend = errors.New("unsupported TSDB storage backend")
)

// Config holds the config information for TSDB storage
type Config struct {
	Dir          string        `yaml:"dir"`
	SyncDir      string        `yaml:"sync_dir"`
	BlockRanges  time.Duration `yaml:"block_ranges_period"`
	Retention    time.Duration `yaml:"retention_period"`
	ShipInterval time.Duration `yaml:"ship_interval"`
	Backend      string        `yaml:"backend"`

	// Multi Bucket option
	MultiS3 backend.MultiBucketConfig `yaml:"s3_multi"`

	// Backends
	S3  s3.Config  `yaml:"s3"`
	GCS gcs.Config `yaml:"gcs"`
}

// RegisterFlags registers the TSDB flags
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.S3.RegisterFlags(f)
	cfg.MultiS3.RegisterFlags(f)

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
		return errUnsupportedBackend
	}

	return nil
}
