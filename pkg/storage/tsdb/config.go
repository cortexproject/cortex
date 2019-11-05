package tsdb

import (
	"errors"
	"flag"
	"strings"
	"time"

	"github.com/alecthomas/units"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/backend/gcs"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/backend/s3"
	"github.com/go-kit/kit/log"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
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
	Dir                  string        `yaml:"dir"`
	SyncDir              string        `yaml:"sync_dir"`
	BlockRanges          DurationList  `yaml:"block_ranges_period"`
	Retention            time.Duration `yaml:"retention_period"`
	ShipInterval         time.Duration `yaml:"ship_interval"`
	Backend              string        `yaml:"backend"`
	UseMemcachedIndex    bool          `yaml:"use_memcached_index"`
	IndexMemcachedConfig `yaml:"index_memcached_cache"`

	// Backends
	S3  s3.Config  `yaml:"s3"`
	GCS gcs.Config `yaml:"gcs"`
}

// DurationList is the block ranges for a tsdb
type DurationList []time.Duration

// String implements the flag.Var interface
func (d DurationList) String() string { return "RangeList is the block ranges for a tsdb" }

// Set implements the flag.Var interface
func (d DurationList) Set(s string) error {
	blocks := strings.Split(s, ",")
	d = make([]time.Duration, 0, len(blocks)) // flag.Parse may be called twice, so overwrite instead of append
	for _, blk := range blocks {
		t, err := time.ParseDuration(blk)
		if err != nil {
			return err
		}
		d = append(d, t)
	}
	return nil
}

// ToMillisecondRanges returns the duration list in milliseconds
func (d DurationList) ToMillisecondRanges() []int64 {
	ranges := make([]int64, 0, len(d))
	for _, t := range d {
		ranges = append(ranges, int64(t/time.Millisecond))
	}

	return ranges
}

// RegisterFlags registers the TSDB flags
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.S3.RegisterFlags(f)
	cfg.GCS.RegisterFlags(f)
	cfg.IndexMemcachedConfig.RegisterFlags(f)

	if len(cfg.BlockRanges) == 0 {
		cfg.BlockRanges = []time.Duration{2 * time.Hour} // Default 2h block
	}

	f.StringVar(&cfg.Dir, "experimental.tsdb.dir", "tsdb", "directory to place all TSDB's into")
	f.StringVar(&cfg.SyncDir, "experimental.tsdb.sync-dir", "tsdb-sync", "directory to place synced tsdb indicies")
	f.Var(cfg.BlockRanges, "experimental.tsdb.block-ranges-period", "comma separated list of TSDB block ranges in time.Duration format")
	f.DurationVar(&cfg.Retention, "experimental.tsdb.retention-period", 6*time.Hour, "TSDB block retention")
	f.DurationVar(&cfg.ShipInterval, "experimental.tsdb.ship-interval", 30*time.Second, "the frequency at which tsdb blocks are scanned for shipping")
	f.StringVar(&cfg.Backend, "experimental.tsdb.backend", "s3", "TSDB storage backend to use")
	f.BoolVar(&cfg.UseMemcachedIndex, "experimental.tsdb.use-memcached-index", false, "Use memcached to store the index cache")
}

// Validate the config
func (cfg *Config) Validate() error {
	if cfg.Backend != BackendS3 && cfg.Backend != BackendGCS {
		return errUnsupportedBackend
	}

	return nil
}

// NewIndexCache returns a new index cache specific by config
func NewIndexCache(logger log.Logger, cfg *Config) (IndexCache, error) {
	if cfg.UseMemcachedIndex {
		return NewMemcachedIndex(cfg.IndexMemcachedConfig)
	}

	indexCacheSizeBytes := uint64(250 * units.Mebibyte)
	maxItemSizeBytes := indexCacheSizeBytes / 2
	return storecache.NewIndexCache(logger, nil, storecache.Opts{
		MaxSizeBytes:     indexCacheSizeBytes,
		MaxItemSizeBytes: maxItemSizeBytes,
	})
}
