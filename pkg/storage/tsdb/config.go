package tsdb

import (
	"errors"
	"flag"
	"strings"
	"time"

	"github.com/cortexproject/cortex/pkg/storage/tsdb/backend/gcs"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/backend/s3"
)

// Constants for the config values
const (
	BackendS3  = "s3"
	BackendGCS = "gcs"
)

// Validation errors
var (
	errUnsupportedBackend = errors.New("unsupported TSDB storage backend")
)

// Config holds the config information for TSDB storage
type Config struct {
	Dir          string        `yaml:"dir"`
	SyncDir      string        `yaml:"sync_dir"`
	BlockRanges  RangeList     `yaml:"block_ranges_period"`
	Retention    time.Duration `yaml:"retention_period"`
	ShipInterval time.Duration `yaml:"ship_interval"`
	Backend      string        `yaml:"backend"`

	// Backends
	S3  s3.Config  `yaml:"s3"`
	GCS gcs.Config `yaml:"gcs"`
}

// RangeList is the block ranges for a tsdb
type RangeList []time.Duration

// String implements the flag.Var interface
func (b RangeList) String() string { return "RangeList is the block ranges for a tsdb" }

// Set implements the flag.Var interface
func (b RangeList) Set(s string) error {
	blocks := strings.Split(s, ",")
	for _, blk := range blocks {
		t, err := time.ParseDuration(blk)
		if err != nil {
			return err
		}
		b = append(b, t)
	}
	return nil
}

// RegisterFlags registers the TSDB flags
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.S3.RegisterFlags(f)
	cfg.GCS.RegisterFlags(f)
	cfg.BlockRanges = []time.Duration{2 * time.Hour} // Default 2h block

	f.StringVar(&cfg.Dir, "experimental.tsdb.dir", "tsdb", "directory to place all TSDB's into")
	f.StringVar(&cfg.SyncDir, "experimental.tsdb.sync-dir", "tsdb-sync", "directory to place synced tsdb indicies")
	f.Var(cfg.BlockRanges, "experimental.tsdb.block-ranges-period", "comma separated list of TSDB block ranges in time.Duration format")
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
