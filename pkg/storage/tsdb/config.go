package tsdb

import (
	"errors"
	"flag"
	"path/filepath"
	"strings"
	"time"

	"github.com/alecthomas/units"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/backend/gcs"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/backend/s3"
)

const (
	// BackendS3 is the value for the S3 storage backend
	BackendS3 = "s3"

	// BackendGCS is the value for the GCS storage backend
	BackendGCS = "gcs"

	// TenantIDExternalLabel is the external label set when shipping blocks to the storage
	TenantIDExternalLabel = "__org_id__"
)

// Validation errors
var (
	errUnsupportedBackend     = errors.New("unsupported TSDB storage backend")
	errInvalidShipConcurrency = errors.New("invalid TSDB ship concurrency")
)

// Config holds the config information for TSDB storage
type Config struct {
	Dir             string            `yaml:"dir"`
	BlockRanges     DurationList      `yaml:"block_ranges_period"`
	Retention       time.Duration     `yaml:"retention_period"`
	ShipInterval    time.Duration     `yaml:"ship_interval"`
	ShipConcurrency int               `yaml:"ship_concurrency"`
	Backend         string            `yaml:"backend"`
	BucketStore     BucketStoreConfig `yaml:"bucket_store"`

	// MaxTSDBOpeningConcurrencyOnStartup limits the number of concurrently opening TSDB's during startup
	MaxTSDBOpeningConcurrencyOnStartup int `yaml:"max_tsdb_opening_concurrency_on_startup"`

	// Backends
	S3  s3.Config  `yaml:"s3"`
	GCS gcs.Config `yaml:"gcs"`
}

// DurationList is the block ranges for a tsdb
type DurationList []time.Duration

// String implements the flag.Value interface
func (d *DurationList) String() string {
	values := make([]string, 0, len(*d))
	for _, v := range *d {
		values = append(values, v.String())
	}

	return strings.Join(values, ",")
}

// Set implements the flag.Value interface
func (d *DurationList) Set(s string) error {
	values := strings.Split(s, ",")
	*d = make([]time.Duration, 0, len(values)) // flag.Parse may be called twice, so overwrite instead of append
	for _, v := range values {
		t, err := time.ParseDuration(v)
		if err != nil {
			return err
		}
		*d = append(*d, t)
	}
	return nil
}

// ToMilliseconds returns the duration list in milliseconds
func (d *DurationList) ToMilliseconds() []int64 {
	values := make([]int64, 0, len(*d))
	for _, t := range *d {
		values = append(values, t.Milliseconds())
	}

	return values
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
	f.Var(&cfg.BlockRanges, "experimental.tsdb.block-ranges-period", "comma separated list of TSDB block ranges in time.Duration format")
	f.DurationVar(&cfg.Retention, "experimental.tsdb.retention-period", 6*time.Hour, "TSDB block retention")
	f.DurationVar(&cfg.ShipInterval, "experimental.tsdb.ship-interval", 1*time.Minute, "How frequently the TSDB blocks are scanned and new ones are shipped to the storage. 0 means shipping is disabled.")
	f.IntVar(&cfg.ShipConcurrency, "experimental.tsdb.ship-concurrency", 10, "Maximum number of tenants concurrently shipping blocks to the storage.")
	f.StringVar(&cfg.Backend, "experimental.tsdb.backend", "s3", "TSDB storage backend to use")
	f.IntVar(&cfg.MaxTSDBOpeningConcurrencyOnStartup, "experimental.tsdb.max-tsdb-opening-concurrency-on-startup", 10, "limit the number of concurrently opening TSDB's on startup")
}

// Validate the config
func (cfg *Config) Validate() error {
	if cfg.Backend != BackendS3 && cfg.Backend != BackendGCS {
		return errUnsupportedBackend
	}

	if cfg.ShipInterval > 0 && cfg.ShipConcurrency <= 0 {
		return errInvalidShipConcurrency
	}

	return nil
}

// BucketStoreConfig holds the config information for Bucket Stores used by the querier
type BucketStoreConfig struct {
	SyncDir               string        `yaml:"sync_dir"`
	SyncInterval          time.Duration `yaml:"sync_interval"`
	IndexCacheSizeBytes   uint64        `yaml:"index_cache_size_bytes"`
	MaxChunkPoolBytes     uint64        `yaml:"max_chunk_pool_bytes"`
	MaxSampleCount        uint64        `yaml:"max_sample_count"`
	MaxConcurrent         int           `yaml:"max_concurrent"`
	TenantSyncConcurrency int           `yaml:"tenant_sync_concurrency"`
	BlockSyncConcurrency  int           `yaml:"block_sync_concurrency"`
	MetaSyncConcurrency   int           `yaml:"meta_sync_concurrency"`
}

// RegisterFlags registers the BucketStore flags
func (cfg *BucketStoreConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.SyncDir, "experimental.tsdb.bucket-store.sync-dir", "tsdb-sync", "Directory to place synced tsdb indicies.")
	f.DurationVar(&cfg.SyncInterval, "experimental.tsdb.bucket-store.sync-interval", 5*time.Minute, "How frequently scan the bucket to look for changes (new blocks shipped by ingesters and blocks removed by retention or compaction). 0 disables it.")
	f.Uint64Var(&cfg.IndexCacheSizeBytes, "experimental.tsdb.bucket-store.index-cache-size-bytes", uint64(250*units.Mebibyte), "Size of index cache in bytes per tenant.")
	f.Uint64Var(&cfg.MaxChunkPoolBytes, "experimental.tsdb.bucket-store.max-chunk-pool-bytes", uint64(2*units.Gibibyte), "Max size of chunk pool in bytes per tenant.")
	f.Uint64Var(&cfg.MaxSampleCount, "experimental.tsdb.bucket-store.max-sample-count", 0, "Max number of samples (0 is no limit) per query when loading series from storage.")
	f.IntVar(&cfg.MaxConcurrent, "experimental.tsdb.bucket-store.max-concurrent", 20, "Max number of concurrent queries to the storage per tenant.")
	f.IntVar(&cfg.TenantSyncConcurrency, "experimental.tsdb.bucket-store.tenant-sync-concurrency", 10, "Maximum number of concurrent tenants synching blocks.")
	f.IntVar(&cfg.BlockSyncConcurrency, "experimental.tsdb.bucket-store.block-sync-concurrency", 20, "Maximum number of concurrent blocks synching per tenant.")
	f.IntVar(&cfg.MetaSyncConcurrency, "experimental.tsdb.bucket-store.meta-sync-concurrency", 20, "Number of Go routines to use when syncing block meta files from object storage per tenant.")
}

// BlocksDir returns the directory path where TSDB blocks and wal should be
// stored by the ingester
func (cfg *Config) BlocksDir(userID string) string {
	return filepath.Join(cfg.Dir, userID)
}
