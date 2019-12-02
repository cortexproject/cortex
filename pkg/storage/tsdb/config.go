package tsdb

import (
	"errors"
	"flag"
	"strings"
	"time"

	"github.com/alecthomas/units"
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
	Dir          string            `yaml:"dir"`
	BlockRanges  DurationList      `yaml:"block_ranges_period"`
	Retention    time.Duration     `yaml:"retention_period"`
	ShipInterval time.Duration     `yaml:"ship_interval"`
	Backend      string            `yaml:"backend"`
	BucketStore  BucketStoreConfig `yaml:"bucket_store"`

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
	cfg.BucketStore.RegisterFlags(f)

	if len(cfg.BlockRanges) == 0 {
		cfg.BlockRanges = []time.Duration{2 * time.Hour} // Default 2h block
	}

	f.StringVar(&cfg.Dir, "experimental.tsdb.dir", "tsdb", "directory to place all TSDB's into")
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

// BucketStoreConfig holds the config information for Bucket Stores used by the querier
type BucketStoreConfig struct {
	SyncDir              string `yaml:"sync_dir"`
	IndexCacheSizeBytes  uint64 `yaml:"index_cache_size_bytes"`
	MaxChunkPoolBytes    uint64 `yaml:"max_chunk_pool_bytes"`
	MaxSampleCount       uint64 `yaml:"max_sample_count"`
	MaxConcurrent        int    `yaml:"max_concurrent"`
	DebugLogging         bool   `yaml:"debug_logging"`
	BlockSyncConcurrency int    `yaml:"block_sync_concurrency"`
}

// RegisterFlags registers the BucketStore flags
func (cfg *BucketStoreConfig) RegisterFlags(f *flag.FlagSet) {

	f.StringVar(&cfg.Dir, "experimental.tsdb.bucket-store.sync-dir", "tsdb-sync", "Directory to place synced tsdb indicies.")
	f.Uint64Var(&cfg.IndexCacheSizeBytes, "experimental.tsdb.bucket-store.index-cache-size-bytes", uint64(250*units.Mebibyte), "Size of index cache in bytes per tenant.")
	f.Uint64Var(&cfg.MaxChunkPoolBytes, "experimental.tsdb.bucket-store.max-chunk-pool-bytes", uint64(2*units.Gibibyte), "Max size of chunk pool in bytes per tenant.")
	f.Uint64Var(&cfg.MaxSampleCount, "experimental.tsdb.bucket-store.max-sample-count", 0, "Max number of samples (0 is no limit) per query when loading series from storage.")
	f.IntVar(&cfg.MaxConcurrent, "experimental.tsdb.bucket-store.max-concurrent", 20, "Max number of concurrent queries to the storage per tenant.")
	f.BoolVar(&cfg.DebugLogging, "experimental.tsdb.bucket-store.debug-logging", false, "Turn on debug logging.")
	f.IntVar(&cfg.BlockSyncConcurrency, "experimental.tsdb.bucket-store.block-sync-concurrency", 20, "Number of Go routines to use when syncing blocks from object storage per tenant.")
}
