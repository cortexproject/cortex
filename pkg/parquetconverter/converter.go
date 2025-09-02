package parquetconverter

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/logutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_parquet "github.com/cortexproject/cortex/pkg/storage/parquet"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/users"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util"
	cortex_errors "github.com/cortexproject/cortex/pkg/util/errors"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

const (
	// ringKey is the key under which we store the compactors ring in the KVStore.
	ringKey = "parquet-converter"

	converterMetaPrefix = "converter-meta-"
)

var RingOp = ring.NewOp([]ring.InstanceState{ring.ACTIVE}, nil)

type Config struct {
	MetaSyncConcurrency int           `yaml:"meta_sync_concurrency"`
	ConversionInterval  time.Duration `yaml:"conversion_interval"`
	MaxRowsPerRowGroup  int           `yaml:"max_rows_per_row_group"`
	FileBufferEnabled   bool          `yaml:"file_buffer_enabled"`
	SortColumns         []string      `yaml:"sort_columns"`

	DataDir string `yaml:"data_dir"`

	Ring RingConfig `yaml:"ring"`
}

type Converter struct {
	services.Service
	logger log.Logger
	reg    prometheus.Registerer

	cfg        Config
	storageCfg cortex_tsdb.BlocksStorageConfig

	limits *validation.Overrides

	// Ring used for sharding compactions.
	ringLifecycler         *ring.Lifecycler
	ring                   *ring.Ring
	ringSubservices        *services.Manager
	ringSubservicesWatcher *services.FailureWatcher

	usersScanner users.Scanner

	bkt objstore.Bucket

	// chunk pool
	pool chunkenc.Pool

	// compaction block ranges
	blockRanges []int64

	fetcherMetrics *block.FetcherMetrics

	baseConverterOptions []convert.ConvertOption

	metrics *metrics

	// Keep track of the last owned users.
	// This is not thread safe now.
	lastOwnedUsers map[string]struct{}
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.Ring.RegisterFlags(f)

	f.StringVar(&cfg.DataDir, "parquet-converter.data-dir", "./data", "Local directory path for caching TSDB blocks during parquet conversion.")
	f.IntVar(&cfg.MetaSyncConcurrency, "parquet-converter.meta-sync-concurrency", 20, "Maximum concurrent goroutines for downloading block metadata from object storage.")
	f.IntVar(&cfg.MaxRowsPerRowGroup, "parquet-converter.max-rows-per-row-group", 1e6, "Maximum number of time series per parquet row group. Larger values improve compression but may reduce performance during reads.")
	f.DurationVar(&cfg.ConversionInterval, "parquet-converter.conversion-interval", time.Minute, "How often to check for new TSDB blocks to convert to parquet format.")
	f.BoolVar(&cfg.FileBufferEnabled, "parquet-converter.file-buffer-enabled", true, "Enable disk-based write buffering to reduce memory consumption during parquet file generation.")
	f.Var((*flagext.StringSlice)(&cfg.SortColumns), "parquet-converter.sort-columns", "Configure the sort columns, in order of precedence, to improve query performance. These will be applied during parquet file generation.")
}

func NewConverter(cfg Config, storageCfg cortex_tsdb.BlocksStorageConfig, blockRanges []int64, logger log.Logger, registerer prometheus.Registerer, limits *validation.Overrides) (*Converter, error) {
	bkt, err := bucket.NewClient(context.Background(), storageCfg.Bucket, nil, "parquet-converter", logger, registerer)
	if err != nil {
		return nil, err
	}
	bkt = bucketindex.BucketWithGlobalMarkers(bkt)
	usersScanner, err := users.NewScanner(storageCfg.UsersScanner, bkt, logger, extprom.WrapRegistererWith(prometheus.Labels{"component": "parquet-converter"}, registerer))
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize users scanner")
	}

	return newConverter(cfg, bkt, storageCfg, blockRanges, logger, registerer, limits, usersScanner), err
}

func newConverter(cfg Config, bkt objstore.InstrumentedBucket, storageCfg cortex_tsdb.BlocksStorageConfig, blockRanges []int64, logger log.Logger, registerer prometheus.Registerer, limits *validation.Overrides, usersScanner users.Scanner) *Converter {
	// Create base sort columns with metric name as the primary sort column
	sortColumns := []string{labels.MetricName}
	if len(cfg.SortColumns) > 0 {
		sortColumns = append(sortColumns, cfg.SortColumns...)
	}
	cfg.SortColumns = sortColumns

	c := &Converter{
		cfg:            cfg,
		reg:            registerer,
		storageCfg:     storageCfg,
		logger:         logger,
		limits:         limits,
		usersScanner:   usersScanner,
		pool:           chunkenc.NewPool(),
		blockRanges:    blockRanges,
		fetcherMetrics: block.NewFetcherMetrics(registerer, nil, nil),
		metrics:        newMetrics(registerer),
		bkt:            bkt,
		baseConverterOptions: []convert.ConvertOption{
			convert.WithSortBy(sortColumns...),
			convert.WithColDuration(time.Hour * 8),
			convert.WithRowGroupSize(cfg.MaxRowsPerRowGroup),
		},
	}

	c.Service = services.NewBasicService(c.starting, c.running, c.stopping)
	return c
}

func (c *Converter) starting(ctx context.Context) error {
	lifecyclerCfg := c.cfg.Ring.ToLifecyclerConfig()
	var err error
	c.ringLifecycler, err = ring.NewLifecycler(lifecyclerCfg, ring.NewNoopFlushTransferer(), "parquet-converter", ringKey, true, false, c.logger, prometheus.WrapRegistererWithPrefix("cortex_", c.reg))
	if err != nil {
		return errors.Wrap(err, "unable to initialize converter ring lifecycler")
	}

	c.ring, err = ring.New(lifecyclerCfg.RingConfig, "parquet-converter", ringKey, c.logger, prometheus.WrapRegistererWithPrefix("cortex_", c.reg))
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

	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*3)
	defer cancel()

	if err := ring.WaitInstanceState(ctxWithTimeout, c.ring, c.ringLifecycler.ID, ring.ACTIVE); err != nil {
		level.Error(c.logger).Log("msg", "failed to become ACTIVE in the ring", "err", err, "state", c.ringLifecycler.GetState())
		return err
	}

	return nil
}

func (c *Converter) running(ctx context.Context) error {
	level.Info(c.logger).Log("msg", "parquet-converter started")
	t := time.NewTicker(c.cfg.ConversionInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			level.Info(c.logger).Log("msg", "start scanning users")
			users, err := c.discoverUsers(ctx)
			if err != nil {
				level.Error(c.logger).Log("msg", "failed to scan users", "err", err)
				continue
			}
			ownedUsers := map[string]struct{}{}
			rand.Shuffle(len(users), func(i, j int) {
				users[i], users[j] = users[j], users[i]
			})

			for _, userID := range users {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if !c.limits.ParquetConverterEnabled(userID) {
					// It is possible that parquet is disabled for the userID so we
					// need to check if the user was owned last time.
					c.cleanupMetricsForNotOwnedUser(userID)
					continue
				}

				var ring ring.ReadRing
				ring = c.ring
				shardSize := c.limits.ParquetConverterTenantShardSize(userID)
				if shardSize > 0 {
					dynamicShardSize := util.DynamicShardSize(c.limits.ParquetConverterTenantShardSize(userID), ring.InstancesCount())
					ring = c.ring.ShuffleShard(userID, dynamicShardSize)
				}

				userLogger := util_log.WithUserID(userID, c.logger)

				owned, err := c.ownUser(ring, userID)
				if err != nil {
					level.Error(userLogger).Log("msg", "failed to check if user is owned by the user", "err", err)
					continue
				}
				if !owned {
					c.cleanupMetricsForNotOwnedUser(userID)
					continue
				}

				if markedForDeletion, err := cortex_tsdb.TenantDeletionMarkExists(ctx, c.bkt, userID); err != nil {
					level.Warn(userLogger).Log("msg", "unable to check if user is marked for deletion", "user", userID, "err", err)
					continue
				} else if markedForDeletion {
					c.metrics.deleteMetricsForTenant(userID)
					level.Info(userLogger).Log("msg", "skipping user because it is marked for deletion", "user", userID)
					continue
				}

				ownedUsers[userID] = struct{}{}

				if err = c.convertUser(ctx, userLogger, ring, userID); err != nil {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					if c.isCausedByPermissionDenied(err) {
						level.Warn(userLogger).Log("msg", "skipping convert user due to PermissionDenied", "err", err)
						continue
					}
					level.Error(userLogger).Log("msg", "failed to convert user", "err", err)
				}
			}
			c.lastOwnedUsers = ownedUsers
			c.metrics.ownedUsers.Set(float64(len(ownedUsers)))

			// Delete local files for unowned tenants, if there are any. This cleans up
			// leftover local files for tenants that belong to different converter now,
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
	}
}

func (c *Converter) stopping(_ error) error {
	ctx := context.Background()
	if c.ringSubservices != nil {
		return services.StopManagerAndAwaitStopped(ctx, c.ringSubservices)
	}
	return nil
}

func (c *Converter) discoverUsers(ctx context.Context) ([]string, error) {
	// Only active users are considered for conversion.
	// We still check deleting and deleted users just to clean up metrics.
	active, deleting, deleted, err := c.usersScanner.ScanUsers(ctx)
	for _, userID := range deleting {
		c.cleanupMetricsForNotOwnedUser(userID)
	}
	for _, userID := range deleted {
		c.cleanupMetricsForNotOwnedUser(userID)
	}
	return active, err
}

func (c *Converter) convertUser(ctx context.Context, logger log.Logger, ring ring.ReadRing, userID string) error {
	level.Info(logger).Log("msg", "start converting user")

	uBucket := bucket.NewUserBucketClient(userID, c.bkt, c.limits)

	var blockLister block.Lister
	switch cortex_tsdb.BlockDiscoveryStrategy(c.storageCfg.BucketStore.BlockDiscoveryStrategy) {
	case cortex_tsdb.ConcurrentDiscovery:
		blockLister = block.NewConcurrentLister(logger, uBucket)
	case cortex_tsdb.RecursiveDiscovery:
		blockLister = block.NewRecursiveLister(logger, uBucket)
	case cortex_tsdb.BucketIndexDiscovery:
		if !c.storageCfg.BucketStore.BucketIndex.Enabled {
			return cortex_tsdb.ErrInvalidBucketIndexBlockDiscoveryStrategy
		}
		blockLister = bucketindex.NewBlockLister(logger, c.bkt, userID, c.limits)
	default:
		return cortex_tsdb.ErrBlockDiscoveryStrategy
	}

	ignoreDeletionMarkFilter := block.NewIgnoreDeletionMarkFilter(
		logger,
		uBucket,
		0,
		c.cfg.MetaSyncConcurrency)

	var baseFetcherMetrics block.BaseFetcherMetrics
	baseFetcherMetrics.Syncs = c.fetcherMetrics.Syncs
	// Create the blocks finder.
	fetcher, err := block.NewMetaFetcherWithMetrics(
		logger,
		c.cfg.MetaSyncConcurrency,
		uBucket,
		blockLister,
		c.metaSyncDirForUser(userID),
		&baseFetcherMetrics,
		c.fetcherMetrics,
		[]block.MetadataFilter{ignoreDeletionMarkFilter},
	)
	if err != nil {
		return errors.Wrap(err, "error creating block fetcher")
	}

	blks, _, err := fetcher.Fetch(ctx)
	if err != nil {
		return errors.Wrapf(err, "failed to fetch blocks for user %s", userID)
	}

	blocks := make([]*metadata.Meta, 0, len(blks))
	for _, blk := range blks {
		blocks = append(blocks, blk)
	}

	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].MinTime > blocks[j].MinTime
	})

	for _, b := range blocks {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		ok, err := c.ownBlock(ring, b.ULID.String())
		if err != nil {
			level.Error(logger).Log("msg", "failed to get own block", "block", b.ULID.String(), "err", err)
			continue
		}

		if !ok {
			continue
		}

		marker, err := cortex_parquet.ReadConverterMark(ctx, b.ULID, uBucket, logger)
		if err != nil {
			level.Error(logger).Log("msg", "failed to read marker", "block", b.ULID.String(), "err", err)
			continue
		}

		if marker.Version == cortex_parquet.CurrentVersion {
			continue
		}

		if !cortex_parquet.ShouldConvertBlockToParquet(b.MinTime, b.MaxTime, c.blockRanges) {
			continue
		}

		if err := os.RemoveAll(c.compactRootDir()); err != nil {
			level.Error(logger).Log("msg", "failed to remove work directory", "path", c.compactRootDir(), "err", err)
			if c.checkConvertError(userID, err) {
				return err
			}
			continue
		}

		bdir := filepath.Join(c.compactDirForUser(userID), b.ULID.String())

		level.Info(logger).Log("msg", "downloading block", "block", b.ULID.String(), "dir", bdir)

		if err := block.Download(ctx, logger, uBucket, b.ULID, bdir, objstore.WithFetchConcurrency(10)); err != nil {
			level.Error(logger).Log("msg", "failed to download block", "block", b.ULID.String(), "err", err)
			if c.checkConvertError(userID, err) {
				return err
			}
			continue
		}

		tsdbBlock, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(logger), bdir, c.pool, tsdb.DefaultPostingsDecoderFactory)
		if err != nil {
			level.Error(logger).Log("msg", "failed to open block", "block", b.ULID.String(), "err", err)
			if c.checkConvertError(userID, err) {
				return err
			}
			continue
		}

		level.Info(logger).Log("msg", "converting block", "block", b.ULID.String(), "dir", bdir)
		start := time.Now()

		converterOpts := append(c.baseConverterOptions, convert.WithName(b.ULID.String()))

		userConfiguredSortColumns := c.limits.ParquetConverterSortColumns(userID)
		if len(userConfiguredSortColumns) > 0 {
			sortColumns := []string{labels.MetricName}
			sortColumns = append(sortColumns, userConfiguredSortColumns...)
			converterOpts = append(converterOpts, convert.WithSortBy(sortColumns...))
		}

		if c.cfg.FileBufferEnabled {
			converterOpts = append(converterOpts, convert.WithColumnPageBuffers(parquet.NewFileBufferPool(bdir, "buffers.*")))
		}

		_, err = convert.ConvertTSDBBlock(
			ctx,
			uBucket,
			tsdbBlock.MinTime(),
			tsdbBlock.MaxTime(),
			[]convert.Convertible{tsdbBlock},
			converterOpts...,
		)

		_ = tsdbBlock.Close()

		if err != nil {
			level.Error(logger).Log("msg", "failed to convert block", "block", b.ULID.String(), "err", err)
			if c.checkConvertError(userID, err) {
				return err
			}
			continue
		}
		duration := time.Since(start)
		c.metrics.convertBlockDuration.WithLabelValues(userID).Set(duration.Seconds())
		level.Info(logger).Log("msg", "successfully converted block", "block", b.ULID.String(), "duration", duration)

		if err = cortex_parquet.WriteConverterMark(ctx, b.ULID, uBucket); err != nil {
			level.Error(logger).Log("msg", "failed to write parquet converter marker", "block", b.ULID.String(), "err", err)
			if c.checkConvertError(userID, err) {
				return err
			}
			continue
		}
		duration = time.Since(start)
		level.Info(logger).Log("msg", "successfully uploaded parquet converter marker", "block", b.ULID.String(), "duration", duration)

		c.metrics.convertedBlocks.WithLabelValues(userID).Inc()
		metaAttrs, err := uBucket.Attributes(ctx, path.Join(b.ULID.String(), metadata.MetaFilename))
		if err != nil {
			// Don't check convert error as attributes call is not really part of the convert process.
			level.Error(logger).Log("msg", "failed to get block meta attributes", "block", b.ULID.String(), "err", err)
			continue
		}
		delayMinutes := time.Since(metaAttrs.LastModified).Minutes()
		c.metrics.convertParquetBlockDelay.Observe(delayMinutes)
	}

	return nil
}

func (c *Converter) checkConvertError(userID string, err error) (terminate bool) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || c.isCausedByPermissionDenied(err) {
		terminate = true
	} else {
		c.metrics.convertBlockFailures.WithLabelValues(userID).Inc()
	}
	return
}

func (c *Converter) isCausedByPermissionDenied(err error) bool {
	cause := errors.Cause(err)
	res := cortex_errors.ErrorIs(cause, func(err error) bool {
		return c.isPermissionDeniedErr(err)
	})
	return res
}

func (c *Converter) isPermissionDeniedErr(err error) bool {
	if c.bkt.IsAccessDeniedErr(err) {
		return true
	}
	s, ok := status.FromError(err)
	if !ok {
		return false
	}
	return s.Code() == codes.PermissionDenied
}

func (c *Converter) ownUser(r ring.ReadRing, userId string) (bool, error) {
	if userId == tenant.GlobalMarkersDir {
		// __markers__ is reserved for global markers and no tenant should be allowed to have that name.
		return false, nil
	}
	rs, err := r.GetAllHealthy(RingOp)
	if err != nil {
		return false, err
	}

	return rs.Includes(c.ringLifecycler.Addr), nil
}

func (c *Converter) ownBlock(ring ring.ReadRing, blockId string) (bool, error) {
	// Hash the user ID.
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(blockId))
	userHash := hasher.Sum32()

	// Check whether this compactor instance owns the user.
	rs, err := ring.Get(userHash, RingOp, nil, nil, nil)
	if err != nil {
		return false, err
	}

	if len(rs.Instances) != 1 {
		return false, fmt.Errorf("unexpected number of compactors in the shard (expected 1, got %d)", len(rs.Instances))
	}

	return rs.Instances[0].Addr == c.ringLifecycler.Addr, nil
}

func (c *Converter) cleanupMetricsForNotOwnedUser(userID string) {
	if _, ok := c.lastOwnedUsers[userID]; ok {
		c.metrics.deleteMetricsForTenant(userID)
	}
}

func (c *Converter) compactRootDir() string {
	return filepath.Join(c.cfg.DataDir, "compact")
}

func (c *Converter) compactDirForUser(userID string) string {
	return filepath.Join(c.compactRootDir(), userID)
}

func (c *Converter) metaSyncDirForUser(userID string) string {
	return filepath.Join(c.cfg.DataDir, converterMetaPrefix+userID)
}

// This function returns tenants with meta sync directories found on local disk. On error, it returns nil map.
func (c *Converter) listTenantsWithMetaSyncDirectories() map[string]struct{} {
	result := map[string]struct{}{}

	files, err := os.ReadDir(c.cfg.DataDir)
	if err != nil {
		return nil
	}

	for _, f := range files {
		if !f.IsDir() {
			continue
		}

		if !strings.HasPrefix(f.Name(), converterMetaPrefix) {
			continue
		}

		result[f.Name()[len(converterMetaPrefix):]] = struct{}{}
	}

	return result
}
