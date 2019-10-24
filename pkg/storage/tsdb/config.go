package tsdb

import (
	"errors"
	"flag"
	"time"

	"github.com/cortexproject/cortex/pkg/storage/tsdb/backend/s3"
)

const (
	BackendS3 = "s3"
)

type Config struct {
	Dir          string        `yaml:"dir"`
	BlockRanges  time.Duration `yaml:"block_ranges_period"`
	Retention    time.Duration `yaml:"retention_period"`
	ShipInterval time.Duration `yaml:"ship_interval"`
	Backend      string        `yaml:"backend"`

	// Backends
	S3 s3.Config `yaml:"s3"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.S3.RegisterFlags(f)

	f.StringVar(&cfg.Dir, "tsdb.dir", "tsdb", "directory to place all TSDB's into")
	f.DurationVar(&cfg.BlockRanges, "tsdb.block-ranges-period", 1*time.Hour, "TSDB block ranges")
	f.DurationVar(&cfg.Retention, "tsdb.retention-period", 6*time.Hour, "TSDB block retention")
	f.DurationVar(&cfg.ShipInterval, "tsdb.ship-interval", 30*time.Second, "the frequency at which tsdb blocks are scanned for shipping")
	f.StringVar(&cfg.Backend, "tsdb.backend", "s3", "TSDB storage backend to use")
}

// TODO need to rebase to call it
func (cfg *Config) Validate() error {
	if cfg.Backend != BackendS3 {
		return errors.New("unsupported TSDB storage backend")
	}

	return nil
}
