package compactor

import (
	"context"
	crypto_rand "crypto/rand"
	"flag"
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/errutil"
	"github.com/thanos-io/thanos/pkg/extprom"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/backoff"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

const (
	// ringKey is the key under which we store the compactors ring in the KVStore.
	ringKey = "compactor"

	blocksMarkedForDeletionName = "cortex_compactor_blocks_marked_for_deletion_total"
	blocksMarkedForDeletionHelp = "Total number of blocks marked for deletion in compactor."
)

var (
	errInvalidBlockRanges = "compactor block range periods should be divisible by the previous one, but %s is not divisible by %s"
	RingOp                = ring.NewOp([]ring.InstanceState{ring.ACTIVE}, nil)

	supportedShardingStrategies              = []string{util.ShardingStrategyDefault, util.ShardingStrategyShuffle}
	errInvalidShardingStrategy               = errors.New("invalid sharding strategy")
	errInvalidTenantShardSize                = errors.New("invalid tenant shard size, the value must be greater than 0")
	supportedCompactionStrategies            = []string{util.CompactionStrategyDefault, util.CompactionStrategyPartitioning}
	errInvalidCompactionStrategy             = errors.New("invalid compaction strategy")
	errInvalidCompactionStrategyPartitioning = errors.New("compaction strategy partitioning can only be enabled when shuffle sharding is enabled")

	DefaultBlocksGrouperFactory = func(ctx context.Context, cfg Config, bkt objstore.InstrumentedBucket, logger log.Logger, blocksMarkedForNoCompaction prometheus.Counter, _ prometheus.Counter, _ prometheus.Counter, syncerMetrics *compact.SyncerMetrics, compactorMetrics *compactorMetrics, _ *ring.Ring, _ *ring.Lifecycler, _ Limits, _ string, _ *compact.GatherNoCompactionMarkFilter) compact.Grouper {
		return compact.NewDefaultGrouperWithMetrics(
			logger,
			bkt,
			cfg.AcceptMalformedIndex,
			true, // Enable vertical compaction
			compactorMetrics.compactions,
			compactorMetrics.compactionRunsStarted,
			compactorMetrics.compactionRunsCompleted,
			compactorMetrics.compactionFailures,
			compactorMetrics.verticalCompactions,
			syncerMetrics.BlocksMarkedForDeletion,
			syncerMetrics.GarbageCollectedBlocks,
			blocksMarkedForNoCompaction,
			metadata.NoneFunc,
			cfg.BlockFilesConcurrency,
			cfg.BlocksFetchConcurrency)
	}

	ShuffleShardingGrouperFactory = func(ctx context.Context, cfg Config, bkt objstore.InstrumentedBucket, logger log.Logger, blocksMarkedForNoCompaction prometheus.Counter, blockVisitMarkerReadFailed prometheus.Counter, blockVisitMarkerWriteFailed prometheus.Counter, syncerMetrics *compact.SyncerMetrics, compactorMetrics *compactorMetrics, ring *ring.Ring, ringLifecycle *ring.Lifecycler, limits Limits, userID string, noCompactionMarkFilter *compact.GatherNoCompactionMarkFilter) compact.Grouper {
		if cfg.CompactionStrategy == util.CompactionStrategyPartitioning {
			return NewPartitionCompactionGrouper(ctx, logger, bkt)
		} else {
			return NewShuffleShardingGrouper(
				ctx,
				logger,
				bkt,
				cfg.AcceptMalformedIndex,
				true, // Enable vertical compaction
				blocksMarkedForNoCompaction,
				metadata.NoneFunc,
				syncerMetrics,
				compactorMetrics,
				cfg,
				ring,
				ringLifecycle.Addr,
				ringLifecycle.ID,
				limits,
				userID,
				cfg.BlockFilesConcurrency,
				cfg.BlocksFetchConcurrency,
				cfg.CompactionConcurrency,
				cfg.BlockVisitMarkerTimeout,
				blockVisitMarkerReadFailed,
				blockVisitMarkerWriteFailed,
				noCompactionMarkFilter.NoCompactMarkedBlocks)
		}
	}

	DefaultBlocksCompactorFactory = func(ctx context.Context, cfg Config, logger log.Logger, reg prometheus.Registerer) (compact.Compactor, PlannerFactory, error) {
		compactor, err := tsdb.NewLeveledCompactor(ctx, reg, logger, cfg.BlockRanges.ToMilliseconds(), downsample.NewPool(), nil)
		if err != nil {
			return nil, nil, err
		}

		plannerFactory := func(ctx context.Context, bkt objstore.InstrumentedBucket, logger log.Logger, cfg Config, noCompactionMarkFilter *compact.GatherNoCompactionMarkFilter, ringLifecycle *ring.Lifecycler, _ string, _ prometheus.Counter, _ prometheus.Counter, _ *compactorMetrics) compact.Planner {
			return compact.NewPlanner(logger, cfg.BlockRanges.ToMilliseconds(), noCompactionMarkFilter)
		}

		return compactor, plannerFactory, nil
	}

	ShuffleShardingBlocksCompactorFactory = func(ctx context.Context, cfg Config, logger log.Logger, reg prometheus.Registerer) (compact.Compactor, PlannerFactory, error) {
		compactor, err := tsdb.NewLeveledCompactor(ctx, reg, logger, cfg.BlockRanges.ToMilliseconds(), downsample.NewPool(), nil)
		if err != nil {
			return nil, nil, err
		}

		plannerFactory := func(ctx context.Context, bkt objstore.InstrumentedBucket, logger log.Logger, cfg Config, noCompactionMarkFilter *compact.GatherNoCompactionMarkFilter, ringLifecycle *ring.Lifecycler, userID string, blockVisitMarkerReadFailed prometheus.Counter, blockVisitMarkerWriteFailed prometheus.Counter, compactorMetrics *compactorMetrics) compact.Planner {

			if cfg.CompactionStrategy == util.CompactionStrategyPartitioning {
				return NewPartitionCompactionPlanner(ctx, bkt, logger)
			} else {
				return NewShuffleShardingPlanner(ctx, bkt, logger, cfg.BlockRanges.ToMilliseconds(), noCompactionMarkFilter.NoCompactMarkedBlocks, ringLifecycle.ID, cfg.BlockVisitMarkerTimeout, cfg.BlockVisitMarkerFileUpdateInterval, blockVisitMarkerReadFailed, blockVisitMarkerWriteFailed)
			}
		}
		return compactor, plannerFactory, nil
	}
)

// BlocksGrouperFactory builds and returns the grouper to use to compact a tenant's blocks.
type BlocksGrouperFactory func(
	ctx context.Context,
	cfg Config,
	bkt objstore.InstrumentedBucket,
	logger log.Logger,
	blocksMarkedForNoCompact prometheus.Counter,
	blockVisitMarkerReadFailed prometheus.Counter,
	blockVisitMarkerWriteFailed prometheus.Counter,
	syncerMetrics *compact.SyncerMetrics,
	compactorMetrics *compactorMetrics,
	ring *ring.Ring,
	ringLifecycler *ring.Lifecycler,
	limit Limits,
	userID string,
	noCompactionMarkFilter *compact.GatherNoCompactionMarkFilter,
) compact.Grouper

// BlocksCompactorFactory builds and returns the compactor and planner to use to compact a tenant's blocks.
type BlocksCompactorFactory func(
	ctx context.Context,
	cfg Config,
	logger log.Logger,
	reg prometheus.Registerer,
) (compact.Compactor, PlannerFactory, error)

type PlannerFactory func(
	ctx context.Context,
	bkt objstore.InstrumentedBucket,
	logger log.Logger,
	cfg Config,
	noCompactionMarkFilter *compact.GatherNoCompactionMarkFilter,
	ringLifecycle *ring.Lifecycler,
	userID string,
	blockVisitMarkerReadFailed prometheus.Counter,
	blockVisitMarkerWriteFailed prometheus.Counter,
	compactorMetrics *compactorMetrics,
) compact.Planner

// Limits defines limits used by the Compactor.
type Limits interface {
	CompactorTenantShardSize(userID string) int
}

// Config holds the Compactor config.
type Config struct {
	BlockRanges                           cortex_tsdb.DurationList `yaml:"block_ranges"`
	BlockSyncConcurrency                  int                      `yaml:"block_sync_concurrency"`
	MetaSyncConcurrency                   int                      `yaml:"meta_sync_concurrency"`
	ConsistencyDelay                      time.Duration            `yaml:"consistency_delay"`
	DataDir                               string                   `yaml:"data_dir"`
	CompactionInterval                    time.Duration            `yaml:"compaction_interval"`
	CompactionRetries                     int                      `yaml:"compaction_retries"`
	CompactionConcurrency                 int                      `yaml:"compaction_concurrency"`
	CleanupInterval                       time.Duration            `yaml:"cleanup_interval"`
	CleanupConcurrency                    int                      `yaml:"cleanup_concurrency"`
	DeletionDelay                         time.Duration            `yaml:"deletion_delay"`
	TenantCleanupDelay                    time.Duration            `yaml:"tenant_cleanup_delay"`
	SkipBlocksWithOutOfOrderChunksEnabled bool                     `yaml:"skip_blocks_with_out_of_order_chunks_enabled"`
	BlockFilesConcurrency                 int                      `yaml:"block_files_concurrency"`
	BlocksFetchConcurrency                int                      `yaml:"blocks_fetch_concurrency"`

	// Whether the migration of block deletion marks to the global markers location is enabled.
	BlockDeletionMarksMigrationEnabled bool `yaml:"block_deletion_marks_migration_enabled"`

	EnabledTenants  flagext.StringSliceCSV `yaml:"enabled_tenants"`
	DisabledTenants flagext.StringSliceCSV `yaml:"disabled_tenants"`

	// Compactors sharding.
	ShardingEnabled  bool       `yaml:"sharding_enabled"`
	ShardingStrategy string     `yaml:"sharding_strategy"`
	ShardingRing     RingConfig `yaml:"sharding_ring"`

	// Compaction mode.
	CompactionStrategy string `yaml:"compaction_mode"`

	// No need to add options to customize the retry backoff,
	// given the defaults should be fine, but allow to override
	// it in tests.
	retryMinBackoff time.Duration `yaml:"-"`
	retryMaxBackoff time.Duration `yaml:"-"`

	// Allow downstream projects to customise the blocks compactor.
	BlocksGrouperFactory   BlocksGrouperFactory   `yaml:"-"`
	BlocksCompactorFactory BlocksCompactorFactory `yaml:"-"`

	// Block visit marker file config
	BlockVisitMarkerTimeout            time.Duration `yaml:"block_visit_marker_timeout"`
	BlockVisitMarkerFileUpdateInterval time.Duration `yaml:"block_visit_marker_file_update_interval"`

	// Cleaner visit marker file config
	CleanerVisitMarkerTimeout            time.Duration `yaml:"cleaner_visit_marker_timeout"`
	CleanerVisitMarkerFileUpdateInterval time.Duration `yaml:"cleaner_visit_marker_file_update_interval"`

	AcceptMalformedIndex bool `yaml:"accept_malformed_index"`
	CachingBucketEnabled bool `yaml:"caching_bucket_enabled"`
}

// RegisterFlags registers the Compactor flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.ShardingRing.RegisterFlags(f)

	cfg.BlockRanges = cortex_tsdb.DurationList{2 * time.Hour, 12 * time.Hour, 24 * time.Hour}
	cfg.retryMinBackoff = 10 * time.Second
	cfg.retryMaxBackoff = time.Minute

	f.Var(&cfg.BlockRanges, "compactor.block-ranges", "List of compaction time ranges.")
	f.DurationVar(&cfg.ConsistencyDelay, "compactor.consistency-delay", 0, fmt.Sprintf("Minimum age of fresh (non-compacted) blocks before they are being processed. Malformed blocks older than the maximum of consistency-delay and %s will be removed.", compact.PartialUploadThresholdAge))
	f.IntVar(&cfg.BlockSyncConcurrency, "compactor.block-sync-concurrency", 20, "Number of Go routines to use when syncing block index and chunks files from the long term storage.")
	f.IntVar(&cfg.MetaSyncConcurrency, "compactor.meta-sync-concurrency", 20, "Number of Go routines to use when syncing block meta files from the long term storage.")
	f.StringVar(&cfg.DataDir, "compactor.data-dir", "./data", "Data directory in which to cache blocks and process compactions")
	f.DurationVar(&cfg.CompactionInterval, "compactor.compaction-interval", time.Hour, "The frequency at which the compaction runs")
	f.IntVar(&cfg.CompactionRetries, "compactor.compaction-retries", 3, "How many times to retry a failed compaction within a single compaction run.")
	f.IntVar(&cfg.CompactionConcurrency, "compactor.compaction-concurrency", 1, "Max number of concurrent compactions running.")
	f.DurationVar(&cfg.CleanupInterval, "compactor.cleanup-interval", 15*time.Minute, "How frequently compactor should run blocks cleanup and maintenance, as well as update the bucket index.")
	f.IntVar(&cfg.CleanupConcurrency, "compactor.cleanup-concurrency", 20, "Max number of tenants for which blocks cleanup and maintenance should run concurrently.")
	f.BoolVar(&cfg.ShardingEnabled, "compactor.sharding-enabled", false, "Shard tenants across multiple compactor instances. Sharding is required if you run multiple compactor instances, in order to coordinate compactions and avoid race conditions leading to the same tenant blocks simultaneously compacted by different instances.")
	f.StringVar(&cfg.ShardingStrategy, "compactor.sharding-strategy", util.ShardingStrategyDefault, fmt.Sprintf("The sharding strategy to use. Supported values are: %s.", strings.Join(supportedShardingStrategies, ", ")))
	f.StringVar(&cfg.CompactionStrategy, "compactor.compaction-mode", util.CompactionStrategyDefault, fmt.Sprintf("The compaction strategy to use. Supported values are: %s.", strings.Join(supportedCompactionStrategies, ", ")))
	f.DurationVar(&cfg.DeletionDelay, "compactor.deletion-delay", 12*time.Hour, "Time before a block marked for deletion is deleted from bucket. "+
		"If not 0, blocks will be marked for deletion and compactor component will permanently delete blocks marked for deletion from the bucket. "+
		"If 0, blocks will be deleted straight away. Note that deleting blocks immediately can cause query failures.")
	f.DurationVar(&cfg.TenantCleanupDelay, "compactor.tenant-cleanup-delay", 6*time.Hour, "For tenants marked for deletion, this is time between deleting of last block, and doing final cleanup (marker files, debug files) of the tenant.")
	f.BoolVar(&cfg.BlockDeletionMarksMigrationEnabled, "compactor.block-deletion-marks-migration-enabled", false, "When enabled, at compactor startup the bucket will be scanned and all found deletion marks inside the block location will be copied to the markers global location too. This option can (and should) be safely disabled as soon as the compactor has successfully run at least once.")
	f.BoolVar(&cfg.SkipBlocksWithOutOfOrderChunksEnabled, "compactor.skip-blocks-with-out-of-order-chunks-enabled", false, "When enabled, mark blocks containing index with out-of-order chunks for no compact instead of halting the compaction.")
	f.IntVar(&cfg.BlockFilesConcurrency, "compactor.block-files-concurrency", 10, "Number of goroutines to use when fetching/uploading block files from object storage.")
	f.IntVar(&cfg.BlocksFetchConcurrency, "compactor.blocks-fetch-concurrency", 3, "Number of goroutines to use when fetching blocks from object storage when compacting.")

	f.Var(&cfg.EnabledTenants, "compactor.enabled-tenants", "Comma separated list of tenants that can be compacted. If specified, only these tenants will be compacted by compactor, otherwise all tenants can be compacted. Subject to sharding.")
	f.Var(&cfg.DisabledTenants, "compactor.disabled-tenants", "Comma separated list of tenants that cannot be compacted by this compactor. If specified, and compactor would normally pick given tenant for compaction (via -compactor.enabled-tenants or sharding), it will be ignored instead.")

	f.DurationVar(&cfg.BlockVisitMarkerTimeout, "compactor.block-visit-marker-timeout", 5*time.Minute, "How long block visit marker file should be considered as expired and able to be picked up by compactor again.")
	f.DurationVar(&cfg.BlockVisitMarkerFileUpdateInterval, "compactor.block-visit-marker-file-update-interval", 1*time.Minute, "How frequently block visit marker file should be updated duration compaction.")

	f.DurationVar(&cfg.CleanerVisitMarkerTimeout, "compactor.cleaner-visit-marker-timeout", 10*time.Minute, "How long cleaner visit marker file should be considered as expired and able to be picked up by cleaner again. The value should be smaller than -compactor.cleanup-interval")
	f.DurationVar(&cfg.CleanerVisitMarkerFileUpdateInterval, "compactor.cleaner-visit-marker-file-update-interval", 5*time.Minute, "How frequently cleaner visit marker file should be updated when cleaning user.")

	f.BoolVar(&cfg.AcceptMalformedIndex, "compactor.accept-malformed-index", false, "When enabled, index verification will ignore out of order label names.")
	f.BoolVar(&cfg.CachingBucketEnabled, "compactor.caching-bucket-enabled", false, "When enabled, caching bucket will be used for compactor, except cleaner service, which serves as the source of truth for block status")
}

func (cfg *Config) Validate(limits validation.Limits) error {
	for _, blockRange := range cfg.BlockRanges {
		if blockRange == 0 {
			return errors.New("compactor block range period cannot be zero")
		}
	}
	// Each block range period should be divisible by the previous one.
	for i := 1; i < len(cfg.BlockRanges); i++ {
		if cfg.BlockRanges[i]%cfg.BlockRanges[i-1] != 0 {
			return errors.Errorf(errInvalidBlockRanges, cfg.BlockRanges[i].String(), cfg.BlockRanges[i-1].String())
		}
	}

	// Make sure a valid sharding strategy is being used
	if !util.StringsContain(supportedShardingStrategies, cfg.ShardingStrategy) {
		return errInvalidShardingStrategy
	}

	if cfg.ShardingEnabled && cfg.ShardingStrategy == util.ShardingStrategyShuffle {
		if limits.CompactorTenantShardSize <= 0 {
			return errInvalidTenantShardSize
		}
	}

	// Make sure a valid compaction mode is being used
	if !util.StringsContain(supportedCompactionStrategies, cfg.CompactionStrategy) {
		return errInvalidCompactionStrategy
	}

	if !cfg.ShardingEnabled && cfg.CompactionStrategy == util.CompactionStrategyPartitioning {
		return errInvalidCompactionStrategyPartitioning
	}

	return nil
}

// ConfigProvider defines the per-tenant config provider for the Compactor.
type ConfigProvider interface {
	bucket.TenantConfigProvider
	CompactorBlocksRetentionPeriod(user string) time.Duration
}

// Compactor is a multi-tenant TSDB blocks compactor based on Thanos.
type Compactor struct {
	services.Service

	compactorCfg   Config
	storageCfg     cortex_tsdb.BlocksStorageConfig
	logger         log.Logger
	parentLogger   log.Logger
	registerer     prometheus.Registerer
	allowedTenants *util.AllowedTenants
	limits         *validation.Overrides

	// Functions that creates bucket client, grouper, planner and compactor using the context.
	// Useful for injecting mock objects from tests.
	bucketClientFactory    func(ctx context.Context) (objstore.InstrumentedBucket, error)
	blocksGrouperFactory   BlocksGrouperFactory
	blocksCompactorFactory BlocksCompactorFactory

	// Users scanner, used to discover users from the bucket.
	usersScanner *cortex_tsdb.UsersScanner

	// Blocks cleaner is responsible to hard delete blocks marked for deletion.
	blocksCleaner *BlocksCleaner

	// Underlying compactor used to compact TSDB blocks.
	blocksCompactor compact.Compactor

	blocksPlannerFactory PlannerFactory

	// Client used to run operations on the bucket storing blocks.
	bucketClient objstore.InstrumentedBucket

	// Ring used for sharding compactions.
	ringLifecycler         *ring.Lifecycler
	ring                   *ring.Ring
	ringSubservices        *services.Manager
	ringSubservicesWatcher *services.FailureWatcher

	// Metrics.
	CompactorStartDurationSeconds  prometheus.Gauge
	CompactionRunsStarted          prometheus.Counter
	CompactionRunsInterrupted      prometheus.Counter
	CompactionRunsCompleted        prometheus.Counter
	CompactionRunsFailed           prometheus.Counter
	CompactionRunsLastSuccess      prometheus.Gauge
	CompactionRunDiscoveredTenants prometheus.Gauge
	CompactionRunSkippedTenants    prometheus.Gauge
	CompactionRunSucceededTenants  prometheus.Gauge
	CompactionRunFailedTenants     prometheus.Gauge
	CompactionRunInterval          prometheus.Gauge
	BlocksMarkedForNoCompaction    prometheus.Counter
	blockVisitMarkerReadFailed     prometheus.Counter
	blockVisitMarkerWriteFailed    prometheus.Counter

	// Thanos compactor metrics per user
	compactorMetrics *compactorMetrics
}

// NewCompactor makes a new Compactor.
func NewCompactor(compactorCfg Config, storageCfg cortex_tsdb.BlocksStorageConfig, logger log.Logger, registerer prometheus.Registerer, limits *validation.Overrides) (*Compactor, error) {
	bucketClientFactory := func(ctx context.Context) (objstore.InstrumentedBucket, error) {
		return bucket.NewClient(ctx, storageCfg.Bucket, "compactor", logger, registerer)
	}

	blocksGrouperFactory := compactorCfg.BlocksGrouperFactory
	if blocksGrouperFactory == nil {
		if compactorCfg.ShardingStrategy == util.ShardingStrategyShuffle {
			blocksGrouperFactory = ShuffleShardingGrouperFactory
		} else {
			blocksGrouperFactory = DefaultBlocksGrouperFactory
		}
	}

	blocksCompactorFactory := compactorCfg.BlocksCompactorFactory
	if blocksCompactorFactory == nil {
		if compactorCfg.ShardingStrategy == util.ShardingStrategyShuffle {
			blocksCompactorFactory = ShuffleShardingBlocksCompactorFactory
		} else {
			blocksCompactorFactory = DefaultBlocksCompactorFactory
		}
	}

	cortexCompactor, err := newCompactor(compactorCfg, storageCfg, logger, registerer, bucketClientFactory, blocksGrouperFactory, blocksCompactorFactory, limits)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Cortex blocks compactor")
	}

	return cortexCompactor, nil
}

func newCompactor(
	compactorCfg Config,
	storageCfg cortex_tsdb.BlocksStorageConfig,
	logger log.Logger,
	registerer prometheus.Registerer,
	bucketClientFactory func(ctx context.Context) (objstore.InstrumentedBucket, error),
	blocksGrouperFactory BlocksGrouperFactory,
	blocksCompactorFactory BlocksCompactorFactory,
	limits *validation.Overrides,
) (*Compactor, error) {
	var compactorMetrics *compactorMetrics
	if compactorCfg.ShardingStrategy == util.ShardingStrategyShuffle {
		compactorMetrics = newCompactorMetrics(registerer)
	} else {
		compactorMetrics = newDefaultCompactorMetrics(registerer)
	}
	c := &Compactor{
		compactorCfg:           compactorCfg,
		storageCfg:             storageCfg,
		parentLogger:           logger,
		logger:                 log.With(logger, "component", "compactor"),
		registerer:             registerer,
		bucketClientFactory:    bucketClientFactory,
		blocksGrouperFactory:   blocksGrouperFactory,
		blocksCompactorFactory: blocksCompactorFactory,
		allowedTenants:         util.NewAllowedTenants(compactorCfg.EnabledTenants, compactorCfg.DisabledTenants),

		CompactorStartDurationSeconds: promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_compactor_start_duration_seconds",
			Help: "Time in seconds spent by compactor running start function",
		}),
		CompactionRunsStarted: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_runs_started_total",
			Help: "Total number of compaction runs started.",
		}),
		CompactionRunsInterrupted: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_runs_interrupted_total",
			Help: "Total number of compaction runs interrupted.",
		}),
		CompactionRunsCompleted: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_runs_completed_total",
			Help: "Total number of compaction runs successfully completed.",
		}),
		CompactionRunsFailed: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_runs_failed_total",
			Help: "Total number of compaction runs failed.",
		}),
		CompactionRunsLastSuccess: promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_compactor_last_successful_run_timestamp_seconds",
			Help: "Unix timestamp of the last successful compaction run.",
		}),
		CompactionRunDiscoveredTenants: promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_compactor_tenants_discovered",
			Help: "Number of tenants discovered during the current compaction run. Reset to 0 when compactor is idle.",
		}),
		CompactionRunSkippedTenants: promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_compactor_tenants_skipped",
			Help: "Number of tenants skipped during the current compaction run. Reset to 0 when compactor is idle.",
		}),
		CompactionRunSucceededTenants: promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_compactor_tenants_processing_succeeded",
			Help: "Number of tenants successfully processed during the current compaction run. Reset to 0 when compactor is idle.",
		}),
		CompactionRunFailedTenants: promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_compactor_tenants_processing_failed",
			Help: "Number of tenants failed processing during the current compaction run. Reset to 0 when compactor is idle.",
		}),
		CompactionRunInterval: promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_compactor_compaction_interval_seconds",
			Help: "The configured interval on which compaction is run in seconds. Useful when compared to the last successful run metric to accurately detect multiple failed compaction runs.",
		}),
		BlocksMarkedForNoCompaction: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_blocks_marked_for_no_compaction_total",
			Help: "Total number of blocks marked for no compact during a compaction run.",
		}),
		blockVisitMarkerReadFailed: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_block_visit_marker_read_failed",
			Help: "Number of block visit marker file failed to be read.",
		}),
		blockVisitMarkerWriteFailed: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_block_visit_marker_write_failed",
			Help: "Number of block visit marker file failed to be written.",
		}),
		limits:           limits,
		compactorMetrics: compactorMetrics,
	}

	if len(compactorCfg.EnabledTenants) > 0 {
		level.Info(c.logger).Log("msg", "compactor using enabled users", "enabled", strings.Join(compactorCfg.EnabledTenants, ", "))
	}
	if len(compactorCfg.DisabledTenants) > 0 {
		level.Info(c.logger).Log("msg", "compactor using disabled users", "disabled", strings.Join(compactorCfg.DisabledTenants, ", "))
	}

	c.Service = services.NewBasicService(c.starting, c.running, c.stopping)

	if c.registerer != nil {
		// Copied from Thanos, pkg/block/fetcher.go
		promauto.With(c.registerer).NewGaugeFunc(prometheus.GaugeOpts{
			Name: "cortex_compactor_meta_sync_consistency_delay_seconds",
			Help: "Configured consistency delay in seconds.",
		}, func() float64 {
			return c.compactorCfg.ConsistencyDelay.Seconds()
		})
	}

	// The last successful compaction run metric is exposed as seconds since epoch, so we need to use seconds for this metric.
	c.CompactionRunInterval.Set(c.compactorCfg.CompactionInterval.Seconds())

	return c, nil
}

// Start the compactor.
func (c *Compactor) starting(ctx context.Context) error {
	begin := time.Now()
	defer func() {
		c.CompactorStartDurationSeconds.Set(time.Since(begin).Seconds())
		level.Info(c.logger).Log("msg", "compactor started", "duration", time.Since(begin), "duration_ms", time.Since(begin).Milliseconds())
	}()

	var err error

	// Create bucket client.
	c.bucketClient, err = c.bucketClientFactory(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to create bucket client")
	}

	// Create blocks compactor dependencies.
	c.blocksCompactor, c.blocksPlannerFactory, err = c.blocksCompactorFactory(ctx, c.compactorCfg, c.logger, c.registerer)
	if err != nil {
		return errors.Wrap(err, "failed to initialize compactor dependencies")
	}

	// Wrap the bucket client to write block deletion marks in the global location too.
	c.bucketClient = bucketindex.BucketWithGlobalMarkers(c.bucketClient)

	// Create the users scanner.
	c.usersScanner = cortex_tsdb.NewUsersScanner(c.bucketClient, c.ownUserForCleanUp, c.parentLogger)

	var cleanerRingLifecyclerID = "default-cleaner"
	// Initialize the compactors ring if sharding is enabled.
	if c.compactorCfg.ShardingEnabled {
		lifecyclerCfg := c.compactorCfg.ShardingRing.ToLifecyclerConfig()
		c.ringLifecycler, err = ring.NewLifecycler(lifecyclerCfg, ring.NewNoopFlushTransferer(), "compactor", ringKey, true, false, c.logger, prometheus.WrapRegistererWithPrefix("cortex_", c.registerer))
		if err != nil {
			return errors.Wrap(err, "unable to initialize compactor ring lifecycler")
		}

		cleanerRingLifecyclerID = c.ringLifecycler.ID

		c.ring, err = ring.New(lifecyclerCfg.RingConfig, "compactor", ringKey, c.logger, prometheus.WrapRegistererWithPrefix("cortex_", c.registerer))
		if err != nil {
			return errors.Wrap(err, "unable to initialize compactor ring")
		}

		c.ringSubservices, err = services.NewManager(c.ringLifecycler, c.ring)
		if err == nil {
			c.ringSubservicesWatcher = services.NewFailureWatcher()
			c.ringSubservicesWatcher.WatchManager(c.ringSubservices)

			err = services.StartManagerAndAwaitHealthy(ctx, c.ringSubservices)
		}

		if err != nil {
			return errors.Wrap(err, "unable to start compactor ring dependencies")
		}

		// If sharding is enabled we should wait until this instance is
		// ACTIVE within the ring. This MUST be done before starting the
		// any other component depending on the users scanner, because the
		// users scanner depends on the ring (to check whether an user belongs
		// to this shard or not).
		level.Info(c.logger).Log("msg", "waiting until compactor is ACTIVE in the ring")

		ctxWithTimeout, cancel := context.WithTimeout(ctx, c.compactorCfg.ShardingRing.WaitActiveInstanceTimeout)
		defer cancel()
		if err := ring.WaitInstanceState(ctxWithTimeout, c.ring, c.ringLifecycler.ID, ring.ACTIVE); err != nil {
			level.Error(c.logger).Log("msg", "compactor failed to become ACTIVE in the ring", "err", err)
			return err
		}
		level.Info(c.logger).Log("msg", "compactor is ACTIVE in the ring")

		// In the event of a cluster cold start or scale up of 2+ compactor instances at the same
		// time, we may end up in a situation where each new compactor instance starts at a slightly
		// different time and thus each one starts with a different state of the ring. It's better
		// to just wait the ring stability for a short time.
		if c.compactorCfg.ShardingRing.WaitStabilityMinDuration > 0 {
			minWaiting := c.compactorCfg.ShardingRing.WaitStabilityMinDuration
			maxWaiting := c.compactorCfg.ShardingRing.WaitStabilityMaxDuration

			level.Info(c.logger).Log("msg", "waiting until compactor ring topology is stable", "min_waiting", minWaiting.String(), "max_waiting", maxWaiting.String())
			if err := ring.WaitRingStability(ctx, c.ring, RingOp, minWaiting, maxWaiting); err != nil {
				level.Warn(c.logger).Log("msg", "compactor ring topology is not stable after the max waiting time, proceeding anyway")
			} else {
				level.Info(c.logger).Log("msg", "compactor ring topology is stable")
			}
		}
	}

	// Create the blocks cleaner (service).
	c.blocksCleaner = NewBlocksCleaner(BlocksCleanerConfig{
		DeletionDelay:                      c.compactorCfg.DeletionDelay,
		CleanupInterval:                    util.DurationWithJitter(c.compactorCfg.CleanupInterval, 0.1),
		CleanupConcurrency:                 c.compactorCfg.CleanupConcurrency,
		BlockDeletionMarksMigrationEnabled: c.compactorCfg.BlockDeletionMarksMigrationEnabled,
		TenantCleanupDelay:                 c.compactorCfg.TenantCleanupDelay,
	}, c.bucketClient, c.usersScanner, c.limits, c.parentLogger, cleanerRingLifecyclerID, c.registerer, c.compactorCfg.CleanerVisitMarkerTimeout, c.compactorCfg.CleanerVisitMarkerFileUpdateInterval,
		c.compactorMetrics.syncerBlocksMarkedForDeletion)

	// Ensure an initial cleanup occurred before starting the compactor.
	if err := services.StartAndAwaitRunning(ctx, c.blocksCleaner); err != nil {
		c.ringSubservices.StopAsync()
		return errors.Wrap(err, "failed to start the blocks cleaner")
	}

	if c.compactorCfg.CachingBucketEnabled {
		matchers := cortex_tsdb.NewMatchers()
		// Do not cache tenant deletion marker and block deletion marker for compactor
		matchers.SetMetaFileMatcher(func(name string) bool {
			return strings.HasSuffix(name, "/"+metadata.MetaFilename)
		})
		c.bucketClient, err = cortex_tsdb.CreateCachingBucket(cortex_tsdb.ChunksCacheConfig{}, c.storageCfg.BucketStore.MetadataCache, matchers, c.bucketClient, c.logger, extprom.WrapRegistererWith(prometheus.Labels{"component": "compactor"}, c.registerer))
		if err != nil {
			return errors.Wrap(err, "create caching bucket")
		}
	}
	return nil
}

func (c *Compactor) stopping(_ error) error {
	begin := time.Now()
	defer func() {
		level.Info(c.logger).Log("msg", "compactor stopped", "duration", time.Since(begin), "duration_ms", time.Since(begin).Milliseconds())
	}()

	ctx := context.Background()

	services.StopAndAwaitTerminated(ctx, c.blocksCleaner) //nolint:errcheck
	if c.ringSubservices != nil {
		return services.StopManagerAndAwaitStopped(ctx, c.ringSubservices)
	}
	return nil
}

func (c *Compactor) running(ctx context.Context) error {
	// Run an initial compaction before starting the interval.
	c.compactUsers(ctx)

	ticker := time.NewTicker(util.DurationWithJitter(c.compactorCfg.CompactionInterval, 0.05))
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.compactUsers(ctx)
		case <-ctx.Done():
			return nil
		case err := <-c.ringSubservicesWatcher.Chan():
			return errors.Wrap(err, "compactor subservice failed")
		}
	}
}

func (c *Compactor) compactUsers(ctx context.Context) {
	failed := false
	interrupted := false

	c.CompactionRunsStarted.Inc()

	defer func() {
		// interruptions and successful runs are considered
		// mutually exclusive but we consider a run failed if any
		// tenant runs failed even if later runs are interrupted
		if !interrupted && !failed {
			c.CompactionRunsCompleted.Inc()
			c.CompactionRunsLastSuccess.SetToCurrentTime()
		}
		if interrupted {
			c.CompactionRunsInterrupted.Inc()
		}
		if failed {
			c.CompactionRunsFailed.Inc()
		}

		// Reset progress metrics once done.
		c.CompactionRunDiscoveredTenants.Set(0)
		c.CompactionRunSkippedTenants.Set(0)
		c.CompactionRunSucceededTenants.Set(0)
		c.CompactionRunFailedTenants.Set(0)
	}()

	level.Info(c.logger).Log("msg", "discovering users from bucket")
	users, err := c.discoverUsersWithRetries(ctx)
	if err != nil {
		failed = true
		level.Error(c.logger).Log("msg", "failed to discover users from bucket", "err", err)
		return
	}

	level.Info(c.logger).Log("msg", "discovered users from bucket", "users", len(users))
	c.CompactionRunDiscoveredTenants.Set(float64(len(users)))

	// When starting multiple compactor replicas nearly at the same time, running in a cluster with
	// a large number of tenants, we may end up in a situation where the 1st user is compacted by
	// multiple replicas at the same time. Shuffling users helps reduce the likelihood this will happen.
	rand.Shuffle(len(users), func(i, j int) {
		users[i], users[j] = users[j], users[i]
	})

	// Keep track of users owned by this shard, so that we can delete the local files for all other users.
	ownedUsers := map[string]struct{}{}
	for _, userID := range users {
		// Ensure the context has not been canceled (ie. compactor shutdown has been triggered).
		if ctx.Err() != nil {
			interrupted = true
			level.Info(c.logger).Log("msg", "interrupting compaction of user blocks", "user", userID)
			return
		}

		// Ensure the user ID belongs to our shard.
		if owned, err := c.ownUserForCompaction(userID); err != nil {
			c.CompactionRunSkippedTenants.Inc()
			level.Warn(c.logger).Log("msg", "unable to check if user is owned by this shard", "user", userID, "err", err)
			continue
		} else if !owned {
			c.CompactionRunSkippedTenants.Inc()
			level.Debug(c.logger).Log("msg", "skipping user because it is not owned by this shard", "user", userID)
			continue
		}

		// Skipping compaction if the  bucket index failed to sync due to CMK errors.
		if idxs, err := bucketindex.ReadSyncStatus(ctx, c.bucketClient, userID, util_log.WithUserID(userID, c.logger)); err == nil {
			if idxs.Status == bucketindex.CustomerManagedKeyError {
				c.CompactionRunSkippedTenants.Inc()
				level.Info(c.logger).Log("msg", "skipping compactUser due CustomerManagedKeyError", "user", userID)
				continue
			}
		}

		ownedUsers[userID] = struct{}{}

		if markedForDeletion, err := cortex_tsdb.TenantDeletionMarkExists(ctx, c.bucketClient, userID); err != nil {
			c.CompactionRunSkippedTenants.Inc()
			level.Warn(c.logger).Log("msg", "unable to check if user is marked for deletion", "user", userID, "err", err)
			continue
		} else if markedForDeletion {
			c.CompactionRunSkippedTenants.Inc()
			level.Debug(c.logger).Log("msg", "skipping user because it is marked for deletion", "user", userID)
			continue
		}

		level.Info(c.logger).Log("msg", "starting compaction of user blocks", "user", userID)

		if err = c.compactUserWithRetries(ctx, userID); err != nil {
			// TODO: patch thanos error types to support errors.Is(err, context.Canceled) here
			if ctx.Err() != nil && ctx.Err() == context.Canceled {
				interrupted = true
				level.Info(c.logger).Log("msg", "interrupting compaction of user blocks", "user", userID)
				return
			}

			c.CompactionRunFailedTenants.Inc()
			failed = true
			level.Error(c.logger).Log("msg", "failed to compact user blocks", "user", userID, "err", err)
			continue
		}

		c.CompactionRunSucceededTenants.Inc()
		level.Info(c.logger).Log("msg", "successfully compacted user blocks", "user", userID)
	}

	// Delete local files for unowned tenants, if there are any. This cleans up
	// leftover local files for tenants that belong to different compactors now,
	// or have been deleted completely.
	for userID := range c.listTenantsWithMetaSyncDirectories() {
		if _, owned := ownedUsers[userID]; owned {
			continue
		}

		dir := c.metaSyncDirForUser(userID)
		s, err := os.Stat(dir)
		if err != nil {
			if !os.IsNotExist(err) {
				level.Warn(c.logger).Log("msg", "failed to stat local directory with user data", "dir", dir, "err", err)
			}
			continue
		}

		if s.IsDir() {
			err := os.RemoveAll(dir)
			if err == nil {
				level.Info(c.logger).Log("msg", "deleted directory for user not owned by this shard", "dir", dir)
			} else {
				level.Warn(c.logger).Log("msg", "failed to delete directory for user not owned by this shard", "dir", dir, "err", err)
			}
		}
	}
}

func (c *Compactor) compactUserWithRetries(ctx context.Context, userID string) error {
	var lastErr error

	retries := backoff.New(ctx, backoff.Config{
		MinBackoff: c.compactorCfg.retryMinBackoff,
		MaxBackoff: c.compactorCfg.retryMaxBackoff,
		MaxRetries: c.compactorCfg.CompactionRetries,
	})

	for retries.Ongoing() {
		lastErr = c.compactUser(ctx, userID)
		if lastErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if c.isCausedByPermissionDenied(lastErr) {
			level.Warn(c.logger).Log("msg", "skipping compactUser due to PermissionDenied", "user", userID, "err", lastErr)
			c.compactorMetrics.compactionErrorsCount.WithLabelValues(userID, unauthorizedError).Inc()
			return nil
		}
		if compact.IsHaltError(lastErr) {
			level.Error(c.logger).Log("msg", "compactor returned critical error", "user", userID, "err", lastErr)
			c.compactorMetrics.compactionErrorsCount.WithLabelValues(userID, haltError).Inc()
			return lastErr
		}
		c.compactorMetrics.compactionErrorsCount.WithLabelValues(userID, retriableError).Inc()

		retries.Wait()
	}

	return lastErr
}

func (c *Compactor) compactUser(ctx context.Context, userID string) error {
	bucket := bucket.NewUserBucketClient(userID, c.bucketClient, c.limits)

	reg := prometheus.NewRegistry()

	ulogger := util_log.WithUserID(userID, c.logger)
	ulogger = util_log.WithExecutionID(ulid.MustNew(ulid.Now(), crypto_rand.Reader).String(), ulogger)

	// Filters out duplicate blocks that can be formed from two or more overlapping
	// blocks that fully submatches the source blocks of the older blocks.
	deduplicateBlocksFilter := block.NewDeduplicateFilter(c.compactorCfg.BlockSyncConcurrency)

	// While fetching blocks, we filter out blocks that were marked for deletion by using IgnoreDeletionMarkFilter.
	// No delay is used -- all blocks with deletion marker are ignored, and not considered for compaction.
	ignoreDeletionMarkFilter := block.NewIgnoreDeletionMarkFilter(
		ulogger,
		bucket,
		0,
		c.compactorCfg.MetaSyncConcurrency)

	// Filters out blocks with no compaction maker; blocks can be marked as no compaction for reasons like
	// out of order chunks or index file too big.
	noCompactMarkerFilter := compact.NewGatherNoCompactionMarkFilter(ulogger, bucket, c.compactorCfg.MetaSyncConcurrency)

	var blockLister block.Lister
	switch cortex_tsdb.BlockDiscoveryStrategy(c.storageCfg.BucketStore.BlockDiscoveryStrategy) {
	case cortex_tsdb.ConcurrentDiscovery:
		blockLister = block.NewConcurrentLister(ulogger, bucket)
	case cortex_tsdb.RecursiveDiscovery:
		blockLister = block.NewRecursiveLister(ulogger, bucket)
	case cortex_tsdb.BucketIndexDiscovery:
		if !c.storageCfg.BucketStore.BucketIndex.Enabled {
			return cortex_tsdb.ErrInvalidBucketIndexBlockDiscoveryStrategy
		}
		blockLister = bucketindex.NewBlockLister(ulogger, c.bucketClient, userID, c.limits)
	default:
		return cortex_tsdb.ErrBlockDiscoveryStrategy
	}

	fetcher, err := block.NewMetaFetcherWithMetrics(
		ulogger,
		c.compactorCfg.MetaSyncConcurrency,
		bucket,
		blockLister,
		c.metaSyncDirForUser(userID),
		c.compactorMetrics.getBaseFetcherMetrics(),
		c.compactorMetrics.getMetaFetcherMetrics(),
		// List of filters to apply (order matters).
		[]block.MetadataFilter{
			// Remove the ingester ID because we don't shard blocks anymore, while still
			// honoring the shard ID if sharding was done in the past.
			NewLabelRemoverFilter([]string{cortex_tsdb.IngesterIDExternalLabel}),
			block.NewConsistencyDelayMetaFilter(ulogger, c.compactorCfg.ConsistencyDelay, reg),
			ignoreDeletionMarkFilter,
			deduplicateBlocksFilter,
			noCompactMarkerFilter,
		},
	)
	if err != nil {
		return err
	}

	syncerMetrics := c.compactorMetrics.getSyncerMetrics(userID)
	syncer, err := compact.NewMetaSyncerWithMetrics(
		ulogger,
		syncerMetrics,
		bucket,
		fetcher,
		deduplicateBlocksFilter,
		ignoreDeletionMarkFilter,
	)
	if err != nil {
		return errors.Wrap(err, "failed to create syncer")
	}

	currentCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	compactor, err := compact.NewBucketCompactor(
		ulogger,
		syncer,
		c.blocksGrouperFactory(currentCtx, c.compactorCfg, bucket, ulogger, c.BlocksMarkedForNoCompaction, c.blockVisitMarkerReadFailed, c.blockVisitMarkerWriteFailed, syncerMetrics, c.compactorMetrics, c.ring, c.ringLifecycler, c.limits, userID, noCompactMarkerFilter),
		c.blocksPlannerFactory(currentCtx, bucket, ulogger, c.compactorCfg, noCompactMarkerFilter, c.ringLifecycler, userID, c.blockVisitMarkerReadFailed, c.blockVisitMarkerWriteFailed, c.compactorMetrics),
		c.blocksCompactor,
		c.compactDirForUser(userID),
		bucket,
		c.compactorCfg.CompactionConcurrency,
		c.compactorCfg.SkipBlocksWithOutOfOrderChunksEnabled,
	)
	if err != nil {
		return errors.Wrap(err, "failed to create bucket compactor")
	}

	if err := compactor.Compact(ctx); err != nil {
		return errors.Wrap(err, "compaction")
	}

	// Remove all files on the compact root dir
	// We do this only if there is no error because potentially on the next run we would not have to download
	// everything again.
	if err := os.RemoveAll(c.compactRootDir()); err != nil {
		level.Error(c.logger).Log("msg", "failed to remove compaction work directory", "path", c.compactRootDir(), "err", err)
	}

	return nil
}

func (c *Compactor) discoverUsersWithRetries(ctx context.Context) ([]string, error) {
	var lastErr error

	retries := backoff.New(ctx, backoff.Config{
		MinBackoff: c.compactorCfg.retryMinBackoff,
		MaxBackoff: c.compactorCfg.retryMaxBackoff,
		MaxRetries: c.compactorCfg.CompactionRetries,
	})

	for retries.Ongoing() {
		var users []string

		users, lastErr = c.discoverUsers(ctx)
		if lastErr == nil {
			return users, nil
		}

		retries.Wait()
	}

	return nil, lastErr
}

func (c *Compactor) discoverUsers(ctx context.Context) ([]string, error) {
	var users []string

	err := c.bucketClient.Iter(ctx, "", func(entry string) error {
		users = append(users, strings.TrimSuffix(entry, "/"))
		return nil
	})

	return users, err
}

func (c *Compactor) ownUserForCompaction(userID string) (bool, error) {
	return c.ownUser(userID, false)
}

func (c *Compactor) ownUserForCleanUp(userID string) (bool, error) {
	return c.ownUser(userID, true)
}

func (c *Compactor) ownUser(userID string, isCleanUp bool) (bool, error) {
	if !c.allowedTenants.IsAllowed(userID) {
		return false, nil
	}

	// Always owned if sharding is disabled
	if !c.compactorCfg.ShardingEnabled {
		return true, nil
	}

	// If we aren't cleaning up user blocks, and we are using shuffle-sharding, ownership is determined by a subring
	// Cleanup should only be owned by a single compactor, as there could be race conditions during block deletion
	if !isCleanUp && c.compactorCfg.ShardingStrategy == util.ShardingStrategyShuffle {
		subRing := c.ring.ShuffleShard(userID, c.limits.CompactorTenantShardSize(userID))

		rs, err := subRing.GetAllHealthy(RingOp)
		if err != nil {
			return false, err
		}

		return rs.Includes(c.ringLifecycler.Addr), nil
	}

	// Hash the user ID.
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(userID))
	userHash := hasher.Sum32()

	// Check whether this compactor instance owns the user.
	rs, err := c.ring.Get(userHash, RingOp, nil, nil, nil)
	if err != nil {
		return false, err
	}

	if len(rs.Instances) != 1 {
		return false, fmt.Errorf("unexpected number of compactors in the shard (expected 1, got %d)", len(rs.Instances))
	}

	return rs.Instances[0].Addr == c.ringLifecycler.Addr, nil
}

const compactorMetaPrefix = "compactor-meta-"

// metaSyncDirForUser returns directory to store cached meta files.
// The fetcher stores cached metas in the "meta-syncer/" sub directory,
// but we prefix it with "compactor-meta-" in order to guarantee no clashing with
// the directory used by the Thanos Syncer, whatever is the user ID.
func (c *Compactor) metaSyncDirForUser(userID string) string {
	return filepath.Join(c.compactorCfg.DataDir, compactorMetaPrefix+userID)
}

// compactDirForUser returns the directory to be used to download and compact the blocks for a user
func (c *Compactor) compactDirForUser(userID string) string {
	return filepath.Join(c.compactRootDir(), userID)
}

// compactRootDir returns the root directory to be used to download and compact blocks
func (c *Compactor) compactRootDir() string {
	return filepath.Join(c.compactorCfg.DataDir, "compact")
}

// This function returns tenants with meta sync directories found on local disk. On error, it returns nil map.
func (c *Compactor) listTenantsWithMetaSyncDirectories() map[string]struct{} {
	result := map[string]struct{}{}

	files, err := os.ReadDir(c.compactorCfg.DataDir)
	if err != nil {
		return nil
	}

	for _, f := range files {
		if !f.IsDir() {
			continue
		}

		if !strings.HasPrefix(f.Name(), compactorMetaPrefix) {
			continue
		}

		result[f.Name()[len(compactorMetaPrefix):]] = struct{}{}
	}

	return result
}

func (c *Compactor) isCausedByPermissionDenied(err error) bool {
	cause := errors.Cause(err)
	if compact.IsRetryError(cause) || compact.IsHaltError(cause) {
		cause = errors.Unwrap(cause)
	}
	if multiErr, ok := cause.(errutil.NonNilMultiRootError); ok {
		for _, err := range multiErr {
			if c.isPermissionDeniedErr(err) {
				return true
			}
		}
		return false
	}
	return c.isPermissionDeniedErr(cause)
}

func (c *Compactor) isPermissionDeniedErr(err error) bool {
	if c.bucketClient.IsAccessDeniedErr(err) {
		return true
	}
	s, ok := status.FromError(err)
	if !ok {
		return false
	}
	return s.Code() == codes.PermissionDenied
}
