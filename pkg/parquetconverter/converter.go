package parquetconverter

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/logutil"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

const (
	// ringKey is the key under which we store the compactors ring in the KVStore.
	ringKey = "parquet-converter"
)

var (
	RingOp = ring.NewOp([]ring.InstanceState{ring.ACTIVE}, nil)
)

type Config struct {
	EnabledTenants  flagext.StringSliceCSV `yaml:"enabled_tenants"`
	DisabledTenants flagext.StringSliceCSV `yaml:"disabled_tenants"`
	DataDir         string                 `yaml:"data_dir"`

	Ring RingConfig `yaml:"ring"`
}

type Converter struct {
	services.Service
	logger log.Logger
	reg    prometheus.Registerer

	cfg        Config
	storageCfg cortex_tsdb.BlocksStorageConfig

	allowedTenants *util.AllowedTenants
	limits         *validation.Overrides

	// Blocks loader
	loader *bucketindex.Loader

	// Ring used for sharding compactions.
	ringLifecycler         *ring.Lifecycler
	ring                   *ring.Ring
	ringSubservices        *services.Manager
	ringSubservicesWatcher *services.FailureWatcher

	bkt objstore.InstrumentedBucket

	// chunk pool
	pool chunkenc.Pool

	// compaction block ranges
	blockRanges []int64
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.Ring.RegisterFlags(f)

	f.Var(&cfg.EnabledTenants, "parquet-converter.enabled-tenants", "Comma separated list of tenants that can be converted. If specified, only these tenants will be converted, otherwise all tenants can be converted.")
	f.Var(&cfg.DisabledTenants, "parquet-converter.disabled-tenants", "Comma separated list of tenants that cannot converted.")
}

func NewConverter(cfg Config, storageCfg cortex_tsdb.BlocksStorageConfig, blockRanges []int64, logger log.Logger, registerer prometheus.Registerer, limits *validation.Overrides) *Converter {
	c := &Converter{
		cfg:            cfg,
		reg:            registerer,
		storageCfg:     storageCfg,
		logger:         logger,
		allowedTenants: util.NewAllowedTenants(cfg.EnabledTenants, cfg.DisabledTenants),
		limits:         limits,
		pool:           chunkenc.NewPool(),
		blockRanges:    blockRanges,
	}

	c.Service = services.NewBasicService(c.starting, c.running, c.stopping)
	return c
}

func (c *Converter) starting(ctx context.Context) error {
	bkt, err := bucket.NewClient(ctx, c.storageCfg.Bucket, nil, "parquet-converter", c.logger, c.reg)
	if err != nil {
		return err
	}

	indexLoaderConfig := bucketindex.LoaderConfig{
		CheckInterval:         time.Minute,
		UpdateOnStaleInterval: c.storageCfg.BucketStore.SyncInterval,
		UpdateOnErrorInterval: c.storageCfg.BucketStore.BucketIndex.UpdateOnErrorInterval,
		IdleTimeout:           c.storageCfg.BucketStore.BucketIndex.IdleTimeout,
	}

	c.loader = bucketindex.NewLoader(indexLoaderConfig, bkt, c.limits, util_log.Logger, prometheus.DefaultRegisterer)

	c.bkt = bkt
	lifecyclerCfg := c.cfg.Ring.ToLifecyclerConfig()
	c.ringLifecycler, err = ring.NewLifecycler(lifecyclerCfg, ring.NewNoopFlushTransferer(), "parquet-converter", ringKey, true, false, c.logger, prometheus.WrapRegistererWithPrefix("cortex_", c.reg))

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

	if err := c.loader.StartAsync(context.Background()); err != nil {
		return errors.Wrap(err, "failed to start loader")
	}

	if err := c.loader.AwaitRunning(ctx); err != nil {
		return errors.Wrap(err, "failed to start loader")
	}

	return nil
}

func (c *Converter) running(ctx context.Context) error {
	level.Info(c.logger).Log("msg", "parquet-converter started")
	t := time.NewTicker(time.Second * 10)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			level.Info(c.logger).Log("msg", "start scaning users")
			users, err := c.discoverUsers(ctx)
			if err != nil {
				level.Error(c.logger).Log("msg", "failed to scan users", "err", err)
				continue
			}
			for _, userID := range users {
				owned, err := c.ownUser(userID)
				if err != nil {
					level.Error(c.logger).Log("msg", "failed to check if user is owned by the user", "user", userID, "err", err)
					continue
				}
				if !owned {
					level.Info(c.logger).Log("msg", "user not owned", "user", userID)
					continue
				}
				level.Info(c.logger).Log("msg", "scanned user", "user", userID)
				userLogger := util_log.WithUserID(userID, c.logger)
				var ring ring.ReadRing
				ring = c.ring
				if c.limits.ParquetConverterTenantShardSize(userID) > 0 {
					ring = c.ring.ShuffleShard(userID, c.limits.ParquetConverterTenantShardSize(userID))
				}

				idx, _, err := c.loader.GetIndex(ctx, userID)
				if err != nil {
					level.Error(userLogger).Log("msg", "failed to get index", "err", err)
					continue
				}

				for _, b := range idx.Blocks {
					ok, err := c.ownBlock(ring, b.ID.String())
					if err != nil {
						level.Error(userLogger).Log("msg", "failed to get own block", "block", b.ID.String(), "err", err)
						continue
					}
					if ok {
						marker, err := ReadConverterMark(ctx, b.ID, c.bkt, userLogger)
						if err != nil {
							level.Error(userLogger).Log("msg", "failed to read marker", "block", b.ID.String(), "err", err)
							continue
						}

						if marker.Version == CurrentVersion {
							continue
						}

						// Do not convert 2 hours blocks
						if getBlockTimeRange(b, c.blockRanges) == c.blockRanges[0] {
							continue
						}

						if err := os.RemoveAll(c.compactRootDir()); err != nil {
							level.Error(userLogger).Log("msg", "failed to remove work directory", "path", c.compactRootDir(), "err", err)
						}

						bdir := filepath.Join(c.compactDirForUser(userID), b.ID.String())
						uBucket := bucket.NewUserBucketClient(userID, c.bkt, c.limits)

						level.Info(userLogger).Log("msg", "downloading block", "block", b.ID.String(), "dir", bdir)
						if err := block.Download(ctx, userLogger, uBucket, b.ID, bdir, objstore.WithFetchConcurrency(10)); err != nil {
							level.Error(userLogger).Log("msg", "Error downloading block", "err", err)
							continue
						}

						tsdbBlock, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(userLogger), bdir, c.pool, tsdb.DefaultPostingsDecoderFactory)
						if err != nil {
							level.Error(userLogger).Log("msg", "Error opening block", "err", err)
							continue
						}
						// Add converter logic
						level.Info(userLogger).Log("msg", "converting block", "block", b.ID.String(), "dir", bdir)
						_, err = convert.ConvertTSDBBlock(
							ctx,
							uBucket,
							tsdbBlock.MinTime(),
							tsdbBlock.MaxTime(),
							[]convert.Convertible{tsdbBlock},
							convert.WithSortBy(labels.MetricName),
							convert.WithColDuration(time.Hour*8),
							convert.WithName(b.ID.String()),
						)

						if err != nil {
							level.Error(userLogger).Log("msg", "Error converting block", "err", err)
						}

						err = WriteCompactMark(ctx, b.ID, uBucket)
						if err != nil {
							level.Error(userLogger).Log("msg", "Error writing block", "err", err)
						}
					}
				}
			}

		}
	}
}

func (c *Converter) stopping(_ error) error {
	ctx := context.Background()
	services.StopAndAwaitTerminated(ctx, c.loader) //nolint:errcheck
	if c.ringSubservices != nil {
		return services.StopManagerAndAwaitStopped(ctx, c.ringSubservices)
	}
	return nil
}

func (c *Converter) discoverUsers(ctx context.Context) ([]string, error) {
	var users []string

	err := c.bkt.Iter(ctx, "", func(entry string) error {
		users = append(users, strings.TrimSuffix(entry, "/"))
		return nil
	})

	return users, err
}

func (c *Converter) ownUser(userID string) (bool, error) {
	if !c.allowedTenants.IsAllowed(userID) {
		return false, nil
	}

	if c.limits.ParquetConverterTenantShardSize(userID) <= 0 {
		return true, nil
	}

	subRing := c.ring.ShuffleShard(userID, c.limits.ParquetConverterTenantShardSize(userID))

	rs, err := subRing.GetAllHealthy(RingOp)
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

func (c *Converter) compactRootDir() string {
	return filepath.Join(c.cfg.DataDir, "compact")
}

func (c *Converter) compactDirForUser(userID string) string {
	return filepath.Join(c.compactRootDir(), userID)
}

func getBlockTimeRange(b *bucketindex.Block, timeRanges []int64) int64 {
	timeRange := int64(0)
	// fallback logic to guess block time range based
	// on MaxTime and MinTime
	blockRange := b.MaxTime - b.MinTime
	for _, tr := range timeRanges {
		rangeStart := getRangeStart(b.MinTime, tr)
		rangeEnd := rangeStart + tr
		if tr >= blockRange && rangeEnd >= b.MaxTime {
			timeRange = tr
			break
		}
	}
	return timeRange
}

func getRangeStart(mint int64, tr int64) int64 {
	// Compute start of aligned time range of size tr closest to the current block's start.
	// This code has been copied from TSDB.
	if mint >= 0 {
		return tr * (mint / tr)
	}

	return tr * ((mint - tr + 1) / tr)
}
