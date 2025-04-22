package parquetconverter

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

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

	// Users scanner, used to discover users from the bucket.
	usersScanner *cortex_tsdb.UsersScanner

	// Blocks loader
	loader *bucketindex.Loader

	// Ring used for sharding compactions.
	ringLifecycler         *ring.Lifecycler
	ring                   *ring.Ring
	ringSubservices        *services.Manager
	ringSubservicesWatcher *services.FailureWatcher

	bkt objstore.InstrumentedBucket
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.Ring.RegisterFlags(f)

	f.Var(&cfg.EnabledTenants, "parquet-converter.enabled-tenants", "Comma separated list of tenants that can be converted. If specified, only these tenants will be converted, otherwise all tenants can be converted.")
	f.Var(&cfg.DisabledTenants, "parquet-converter.disabled-tenants", "Comma separated list of tenants that cannot converted.")
}

func NewConverter(cfg Config, storageCfg cortex_tsdb.BlocksStorageConfig, logger log.Logger, registerer prometheus.Registerer, limits *validation.Overrides) *Converter {
	c := &Converter{
		cfg:            cfg,
		reg:            registerer,
		storageCfg:     storageCfg,
		logger:         logger,
		allowedTenants: util.NewAllowedTenants(cfg.EnabledTenants, cfg.DisabledTenants),
		limits:         limits,
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
	c.usersScanner = cortex_tsdb.NewUsersScanner(c.bkt, c.ownUser, c.logger)
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

	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	if err := ring.WaitInstanceState(ctxWithTimeout, c.ring, c.ringLifecycler.ID, ring.ACTIVE); err != nil {
		level.Error(c.logger).Log("msg", "failed to become ACTIVE in the ring", "err", err)
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
	t := time.NewTicker(time.Second * 10)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			users, _, err := c.usersScanner.ScanUsers(ctx)
			if err != nil {
				level.Error(c.logger).Log("msg", "failed to scan users", "err", err)
				continue
			}
			for _, userID := range users {
				var ring ring.ReadRing
				ring = c.ring
				if c.limits.ParquetConverterTenantShardSize(userID) > 0 {
					ring = c.ring.ShuffleShard(userID, c.limits.ParquetConverterTenantShardSize(userID))
				}

				idx, _, err := c.loader.GetIndex(ctx, userID)
				if err != nil {
					level.Error(c.logger).Log("msg", "failed to get index", "userID", userID, "err", err)
					continue
				}

				for _, block := range idx.Blocks {
					ok, err := c.ownBlock(ring, block.ID.String())
					if err != nil {
						level.Error(c.logger).Log("msg", "failed to get own block", "block", block.ID.String(), "err", err)
						continue
					}
					if ok {
						marker, err := ReadConverterMark(ctx, block.ID, c.bkt, c.logger)
						if err != nil {
							level.Error(c.logger).Log("msg", "failed to read marker", "block", block.ID.String(), "err", err)
							continue
						}

						if marker.Version == CurrentVersion {
							continue
						}

						// Add converter logic
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
