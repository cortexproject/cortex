package compactor

import (
	"context"
	"flag"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/objstore"
)

// Config holds the Compactor config.
type Config struct {
	BlockRanges          cortex_tsdb.DurationList `yaml:"block_ranges"`
	BlockSyncConcurrency int                      `yaml:"block_sync_concurrency"`
	ConsistencyDelay     time.Duration            `yaml:"consistency_delay"`
	DataDir              string                   `yaml:"data_dir"`
	CompactionInterval   time.Duration            `yaml:"compaction_interval"`
	CompactionRetries    int                      `yaml:"compaction_retries"`

	// No need to add options to customize the retry backoff,
	// given the defaults should be fine, but allow to override
	// it in tests.
	retryMinBackoff time.Duration `yaml:"-"`
	retryMaxBackoff time.Duration `yaml:"-"`
}

// RegisterFlags registers the Compactor flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.BlockRanges = cortex_tsdb.DurationList{2 * time.Hour, 12 * time.Hour, 24 * time.Hour}
	cfg.retryMinBackoff = 10 * time.Second
	cfg.retryMaxBackoff = time.Minute

	f.Var(
		&cfg.BlockRanges,
		"compactor.block-ranges",
		"Comma separated list of compaction ranges expressed in the time duration format")

	f.DurationVar(
		&cfg.ConsistencyDelay,
		"compactor.consistency-delay",
		30*time.Minute,
		fmt.Sprintf("Minimum age of fresh (non-compacted) blocks before they are being processed. Malformed blocks older than the maximum of consistency-delay and %s will be removed.", compact.MinimumAgeForRemoval))

	f.IntVar(
		&cfg.BlockSyncConcurrency,
		"compactor.block-sync-concurrency",
		20,
		"Number of goroutines to use when syncing block metadata from object storage")

	f.StringVar(
		&cfg.DataDir,
		"compactor.data-dir",
		"./data",
		"Data directory in which to cache blocks and process compactions")

	f.DurationVar(
		&cfg.CompactionInterval,
		"compactor.compaction-interval",
		time.Hour,
		"The frequency at which the compaction runs")

	f.IntVar(
		&cfg.CompactionRetries,
		"compactor.compaction-retries",
		3,
		"How many times to retry a failed compaction during a single compaction interval")
}

// Compactor is a multi-tenant TSDB blocks compactor based on Thanos.
type Compactor struct {
	compactorCfg Config
	storageCfg   cortex_tsdb.Config
	logger       log.Logger

	// Underlying compactor used to compact TSDB blocks.
	tsdbCompactor tsdb.Compactor

	// Client used to run operations on the bucket storing blocks.
	bucketClient objstore.Bucket

	// Channel used to signal when the compactor should stop.
	quit   chan struct{}
	runner sync.WaitGroup

	// Context used to run compaction and its cancel function to
	// safely interrupt it on shutdown.
	ctx       context.Context
	cancelCtx context.CancelFunc

	// Metrics.
	compactionRunsStarted   prometheus.Counter
	compactionRunsCompleted prometheus.Counter
	compactionRunsFailed    prometheus.Counter
}

// NewCompactor makes a new Compactor.
func NewCompactor(compactorCfg Config, storageCfg cortex_tsdb.Config, logger log.Logger, registerer prometheus.Registerer) (*Compactor, error) {
	ctx, cancelCtx := context.WithCancel(context.Background())

	bucketClient, err := cortex_tsdb.NewBucketClient(ctx, storageCfg, "compactor", logger)
	if err != nil {
		cancelCtx()
		return nil, errors.Wrap(err, "failed to create the bucket client")
	}

	tsdbCompactor, err := tsdb.NewLeveledCompactor(ctx, registerer, logger, compactorCfg.BlockRanges.ToMillisecond(), downsample.NewPool())
	if err != nil {
		cancelCtx()
		return nil, errors.Wrap(err, "failed to create TSDB compactor")
	}

	return newCompactor(ctx, cancelCtx, compactorCfg, storageCfg, bucketClient, tsdbCompactor, logger, registerer)
}

func newCompactor(ctx context.Context, cancelCtx context.CancelFunc, compactorCfg Config, storageCfg cortex_tsdb.Config, bucketClient objstore.Bucket, tsdbCompactor tsdb.Compactor, logger log.Logger, registerer prometheus.Registerer) (*Compactor, error) {
	c := &Compactor{
		compactorCfg:  compactorCfg,
		storageCfg:    storageCfg,
		logger:        logger,
		bucketClient:  bucketClient,
		tsdbCompactor: tsdbCompactor,
		quit:          make(chan struct{}),
		ctx:           ctx,
		cancelCtx:     cancelCtx,
		compactionRunsStarted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_runs_started_total",
			Help: "Total number of compaction runs started.",
		}),
		compactionRunsCompleted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_runs_completed_total",
			Help: "Total number of compaction runs successfully completed.",
		}),
		compactionRunsFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_runs_failed_total",
			Help: "Total number of compaction runs failed.",
		}),
	}

	// Register metrics.
	if registerer != nil {
		registerer.MustRegister(c.compactionRunsStarted, c.compactionRunsCompleted, c.compactionRunsFailed)
	}

	// Start the compactor loop.
	c.runner.Add(1)
	go c.run()

	return c, nil
}

// Shutdown the compactor and waits until done. This may take some time
// if there's a on-going compaction.
func (c *Compactor) Shutdown() {
	close(c.quit)
	c.cancelCtx()
	c.runner.Wait()
}

func (c *Compactor) run() {
	defer c.runner.Done()

	// Run an initial compaction before starting the interval.
	c.compactUsersWithRetries(c.ctx)

	ticker := time.NewTicker(c.compactorCfg.CompactionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.compactUsersWithRetries(c.ctx)
		case <-c.quit:
			return
		}
	}
}

func (c *Compactor) compactUsersWithRetries(ctx context.Context) {
	retries := util.NewBackoff(ctx, util.BackoffConfig{
		MinBackoff: c.compactorCfg.retryMinBackoff,
		MaxBackoff: c.compactorCfg.retryMaxBackoff,
		MaxRetries: c.compactorCfg.CompactionRetries,
	})

	c.compactionRunsStarted.Inc()

	for retries.Ongoing() {
		if err := c.compactUsers(ctx); err == nil {
			c.compactionRunsCompleted.Inc()
			return
		}

		retries.Wait()
	}

	c.compactionRunsFailed.Inc()
}

func (c *Compactor) compactUsers(ctx context.Context) error {
	level.Info(c.logger).Log("msg", "discovering users from bucket")
	users, err := c.discoverUsers(ctx)
	if err != nil {
		level.Error(c.logger).Log("msg", "failed to discover users from bucket", "err", err)
		return err
	}
	level.Info(c.logger).Log("msg", "discovered users from bucket", "users", len(users))

	for _, userID := range users {
		// Ensure the context has not been canceled (ie. compactor shutdown has been triggered).
		if ctx.Err() != nil {
			return ctx.Err()
		}

		level.Info(c.logger).Log("msg", "starting compaction of user blocks", "user", userID)

		if err = c.compactUser(ctx, userID); err != nil {
			level.Error(c.logger).Log("msg", "failed to compact user blocks", "user", userID, "err", err)
			continue
		}

		level.Info(c.logger).Log("msg", "successfully compacted user blocks", "user", userID)
	}

	return nil
}

func (c *Compactor) compactUser(ctx context.Context, userID string) error {
	bucket := cortex_tsdb.NewUserBucketClient(userID, c.bucketClient)

	syncer, err := compact.NewSyncer(
		c.logger,
		nil, // TODO(pracucci) we should pass the prometheus registerer, but we would need to inject the user label to each metric, otherwise we have clashing metrics
		bucket,
		c.compactorCfg.ConsistencyDelay,
		c.compactorCfg.BlockSyncConcurrency,
		false, // Do not accept malformed indexes
		true,  // Enable vertical compaction
		[]*relabel.Config{})
	if err != nil {
		return errors.Wrap(err, "failed to create syncer")
	}

	compactor, err := compact.NewBucketCompactor(
		c.logger,
		syncer,
		c.tsdbCompactor,
		path.Join(c.compactorCfg.DataDir, "compact"),
		bucket,
		// No compaction concurrency. Due to how Cortex works we don't
		// expect to have multiple block groups per tenant, so setting
		// a value higher than 1 would be useless.
		1,
	)
	if err != nil {
		return errors.Wrap(err, "failed to create bucket compactor")
	}

	return compactor.Compact(ctx)
}

func (c *Compactor) discoverUsers(ctx context.Context) ([]string, error) {
	users := make([]string, 0)

	err := c.bucketClient.Iter(ctx, "", func(entry string) error {
		users = append(users, strings.TrimSuffix(entry, "/"))
		return nil
	})

	return users, err
}
