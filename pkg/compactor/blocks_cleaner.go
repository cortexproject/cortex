package compactor

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/concurrency"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/services"
)

const (
	defaultDeleteBlocksConcurrency = 16
	reasonValueRetention           = "retention"
	activeStatus                   = "active"
	deletedStatus                  = "deleted"
)

type BlocksCleanerConfig struct {
	DeletionDelay                      time.Duration
	CleanupInterval                    time.Duration
	CleanupConcurrency                 int
	BlockDeletionMarksMigrationEnabled bool          // TODO Discuss whether we should remove it in Cortex 1.8.0 and document that upgrading to 1.7.0 before 1.8.0 is required.
	TenantCleanupDelay                 time.Duration // Delay before removing tenant deletion mark and "debug".
	ShardingStrategy                   string
	CompactionStrategy                 string
}

type BlocksCleaner struct {
	services.Service

	cfg          BlocksCleanerConfig
	cfgProvider  ConfigProvider
	logger       log.Logger
	bucketClient objstore.InstrumentedBucket
	usersScanner *cortex_tsdb.UsersScanner

	ringLifecyclerID string

	// Keep track of the last owned users.
	lastOwnedUsers []string

	cleanerVisitMarkerTimeout            time.Duration
	cleanerVisitMarkerFileUpdateInterval time.Duration
	compactionVisitMarkerTimeout         time.Duration

	// Metrics.
	runsStarted                       *prometheus.CounterVec
	runsCompleted                     *prometheus.CounterVec
	runsFailed                        *prometheus.CounterVec
	runsLastSuccess                   *prometheus.GaugeVec
	blocksCleanedTotal                prometheus.Counter
	blocksFailedTotal                 prometheus.Counter
	blocksMarkedForDeletion           *prometheus.CounterVec
	tenantBlocks                      *prometheus.GaugeVec
	tenantBlocksMarkedForDelete       *prometheus.GaugeVec
	tenantBlocksMarkedForNoCompaction *prometheus.GaugeVec
	tenantPartialBlocks               *prometheus.GaugeVec
	tenantBucketIndexLastUpdate       *prometheus.GaugeVec
	tenantBlocksCleanedTotal          *prometheus.CounterVec
	tenantCleanDuration               *prometheus.GaugeVec
	remainingPlannedCompactions       *prometheus.GaugeVec
	inProgressCompactions             *prometheus.GaugeVec
	oldestPartitionGroupOffset        *prometheus.GaugeVec
}

func NewBlocksCleaner(
	cfg BlocksCleanerConfig,
	bucketClient objstore.InstrumentedBucket,
	usersScanner *cortex_tsdb.UsersScanner,
	compactionVisitMarkerTimeout time.Duration,
	cfgProvider ConfigProvider,
	logger log.Logger,
	ringLifecyclerID string,
	reg prometheus.Registerer,
	cleanerVisitMarkerTimeout time.Duration,
	cleanerVisitMarkerFileUpdateInterval time.Duration,
	blocksMarkedForDeletion *prometheus.CounterVec,
	remainingPlannedCompactions *prometheus.GaugeVec,
) *BlocksCleaner {

	var inProgressCompactions *prometheus.GaugeVec
	var oldestPartitionGroupOffset *prometheus.GaugeVec
	if cfg.ShardingStrategy == util.ShardingStrategyShuffle && cfg.CompactionStrategy == util.CompactionStrategyPartitioning {
		inProgressCompactions = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_compactor_in_progress_compactions",
			Help: "Total number of in progress compactions. Only available with shuffle-sharding strategy and partitioning compaction strategy",
		}, commonLabels)
		oldestPartitionGroupOffset = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_compactor_oldest_partition_offset",
			Help: "Time in seconds between now and the oldest created partition group not completed. Only available with shuffle-sharding strategy and partitioning compaction strategy",
		}, commonLabels)
	}

	c := &BlocksCleaner{
		cfg:                                  cfg,
		bucketClient:                         bucketClient,
		usersScanner:                         usersScanner,
		compactionVisitMarkerTimeout:         compactionVisitMarkerTimeout,
		cfgProvider:                          cfgProvider,
		logger:                               log.With(logger, "component", "cleaner"),
		ringLifecyclerID:                     ringLifecyclerID,
		cleanerVisitMarkerTimeout:            cleanerVisitMarkerTimeout,
		cleanerVisitMarkerFileUpdateInterval: cleanerVisitMarkerFileUpdateInterval,
		runsStarted: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_compactor_block_cleanup_started_total",
			Help: "Total number of blocks cleanup runs started.",
		}, []string{"user_status"}),
		runsCompleted: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_compactor_block_cleanup_completed_total",
			Help: "Total number of blocks cleanup runs successfully completed.",
		}, []string{"user_status"}),
		runsFailed: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_compactor_block_cleanup_failed_total",
			Help: "Total number of blocks cleanup runs failed.",
		}, []string{"user_status"}),
		runsLastSuccess: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_compactor_block_cleanup_last_successful_run_timestamp_seconds",
			Help: "Unix timestamp of the last successful blocks cleanup run.",
		}, []string{"user_status"}),
		blocksCleanedTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_blocks_cleaned_total",
			Help: "Total number of blocks deleted.",
		}),
		blocksFailedTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_block_cleanup_failures_total",
			Help: "Total number of blocks failed to be deleted.",
		}),
		blocksMarkedForDeletion: blocksMarkedForDeletion,

		// The following metrics don't have the "cortex_compactor" prefix because not strictly related to
		// the compactor. They're just tracked by the compactor because it's the most logical place where these
		// metrics can be tracked.
		tenantBlocks: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_bucket_blocks_count",
			Help: "Total number of blocks in the bucket. Includes blocks marked for deletion, but not partial blocks.",
		}, commonLabels),
		tenantBlocksMarkedForDelete: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_bucket_blocks_marked_for_deletion_count",
			Help: "Total number of blocks marked for deletion in the bucket.",
		}, commonLabels),
		tenantBlocksMarkedForNoCompaction: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_bucket_blocks_marked_for_no_compaction_count",
			Help: "Total number of blocks marked for no compaction in the bucket.",
		}, commonLabels),
		tenantPartialBlocks: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_bucket_blocks_partials_count",
			Help: "Total number of partial blocks.",
		}, commonLabels),
		tenantBucketIndexLastUpdate: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_bucket_index_last_successful_update_timestamp_seconds",
			Help: "Timestamp of the last successful update of a tenant's bucket index.",
		}, commonLabels),
		tenantBlocksCleanedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_bucket_blocks_cleaned_total",
			Help: "Total number of blocks deleted for a tenant.",
		}, commonLabels),
		tenantCleanDuration: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_bucket_clean_duration_seconds",
			Help: "Duration of cleaner runtime for a tenant in seconds",
		}, commonLabels),
		remainingPlannedCompactions: remainingPlannedCompactions,
		inProgressCompactions:       inProgressCompactions,
		oldestPartitionGroupOffset:  oldestPartitionGroupOffset,
	}

	c.Service = services.NewBasicService(c.starting, c.loop, nil)

	return c
}

type cleanerJob struct {
	users     []string
	timestamp int64
}

func (c *BlocksCleaner) starting(ctx context.Context) error {
	// Run a cleanup so that any other service depending on this service
	// is guaranteed to start once the initial cleanup has been done.
	activeUsers, deletedUsers, err := c.scanUsers(ctx)

	if err != nil {
		level.Error(c.logger).Log("msg", "failed to scan users on startup", "err", err.Error())
		c.runsFailed.WithLabelValues(deletedStatus).Inc()
		c.runsFailed.WithLabelValues(activeStatus).Inc()
		return nil
	}
	err = c.cleanUpActiveUsers(ctx, activeUsers, true)
	c.checkRunError(activeStatus, err)
	err = c.cleanDeletedUsers(ctx, deletedUsers)
	c.checkRunError(deletedStatus, err)
	return nil
}

func (c *BlocksCleaner) loop(ctx context.Context) error {
	t := time.NewTicker(c.cfg.CleanupInterval)
	defer t.Stop()

	usersChan := make(chan *cleanerJob)
	deleteChan := make(chan *cleanerJob)
	defer close(usersChan)
	defer close(deleteChan)

	go func() {
		c.runActiveUserCleanup(ctx, usersChan)
	}()
	go func() {
		c.runDeleteUserCleanup(ctx, deleteChan)
	}()

	for {
		select {
		case <-t.C:
			activeUsers, deletedUsers, err := c.scanUsers(ctx)
			if err != nil {
				level.Error(c.logger).Log("msg", "failed to scan users blocks cleanup and maintenance", "err", err.Error())
				c.runsFailed.WithLabelValues(deletedStatus).Inc()
				c.runsFailed.WithLabelValues(activeStatus).Inc()
				continue
			}
			cleanJobTimestamp := time.Now().Unix()
			usersChan <- &cleanerJob{
				users:     activeUsers,
				timestamp: cleanJobTimestamp,
			}
			deleteChan <- &cleanerJob{
				users:     deletedUsers,
				timestamp: cleanJobTimestamp,
			}

		case <-ctx.Done():
			return nil
		}
	}
}

func (c *BlocksCleaner) checkRunError(runType string, err error) {
	if err == nil {
		level.Info(c.logger).Log("msg", fmt.Sprintf("successfully completed blocks cleanup and maintenance for %s users", runType))
		c.runsCompleted.WithLabelValues(runType).Inc()
		c.runsLastSuccess.WithLabelValues(runType).SetToCurrentTime()
	} else if errors.Is(err, context.Canceled) {
		level.Info(c.logger).Log("msg", fmt.Sprintf("canceled blocks cleanup and maintenance for %s users", runType), "err", err)
	} else {
		level.Error(c.logger).Log("msg", fmt.Sprintf("failed to run blocks cleanup and maintenance for %s users", runType), "err", err.Error())
		c.runsFailed.WithLabelValues(runType).Inc()
	}
}

func (c *BlocksCleaner) runActiveUserCleanup(ctx context.Context, jobChan chan *cleanerJob) {
	for job := range jobChan {
		if job.timestamp < time.Now().Add(-c.cfg.CleanupInterval).Unix() {
			level.Warn(c.logger).Log("Active user cleaner job too old. Ignoring to get recent data")
			continue
		}
		err := c.cleanUpActiveUsers(ctx, job.users, false)

		c.checkRunError(activeStatus, err)
	}
}

func (c *BlocksCleaner) cleanUpActiveUsers(ctx context.Context, users []string, firstRun bool) error {
	level.Info(c.logger).Log("msg", "started blocks cleanup and maintenance for active users")
	c.runsStarted.WithLabelValues(activeStatus).Inc()

	return concurrency.ForEachUser(ctx, users, c.cfg.CleanupConcurrency, func(ctx context.Context, userID string) error {
		userLogger := util_log.WithUserID(userID, c.logger)
		userBucket := bucket.NewUserBucketClient(userID, c.bucketClient, c.cfgProvider)
		visitMarkerManager, isVisited, err := c.obtainVisitMarkerManager(ctx, userLogger, userBucket)
		if err != nil {
			return err
		}
		if isVisited {
			return nil
		}
		errChan := make(chan error, 1)
		go visitMarkerManager.HeartBeat(ctx, errChan, c.cleanerVisitMarkerFileUpdateInterval, true)
		defer func() {
			errChan <- nil
		}()
		return errors.Wrapf(c.cleanUser(ctx, userLogger, userBucket, userID, firstRun), "failed to delete blocks for user: %s", userID)
	})
}

func (c *BlocksCleaner) runDeleteUserCleanup(ctx context.Context, jobChan chan *cleanerJob) {
	for job := range jobChan {
		if job.timestamp < time.Now().Add(-c.cfg.CleanupInterval).Unix() {
			level.Warn(c.logger).Log("Delete users cleaner job too old. Ignoring to get recent data")
			continue
		}
		err := c.cleanDeletedUsers(ctx, job.users)

		c.checkRunError(deletedStatus, err)
	}
}

func (c *BlocksCleaner) cleanDeletedUsers(ctx context.Context, users []string) error {
	level.Info(c.logger).Log("msg", "started blocks cleanup and maintenance for deleted users")
	c.runsStarted.WithLabelValues(deletedStatus).Inc()

	return concurrency.ForEachUser(ctx, users, c.cfg.CleanupConcurrency, func(ctx context.Context, userID string) error {
		userLogger := util_log.WithUserID(userID, c.logger)
		userBucket := bucket.NewUserBucketClient(userID, c.bucketClient, c.cfgProvider)
		visitMarkerManager, isVisited, err := c.obtainVisitMarkerManager(ctx, userLogger, userBucket)
		if err != nil {
			return err
		}
		if isVisited {
			return nil
		}
		errChan := make(chan error, 1)
		go visitMarkerManager.HeartBeat(ctx, errChan, c.cleanerVisitMarkerFileUpdateInterval, true)
		defer func() {
			errChan <- nil
		}()
		return errors.Wrapf(c.deleteUserMarkedForDeletion(ctx, userLogger, userBucket, userID), "failed to delete user marked for deletion: %s", userID)
	})
}

func (c *BlocksCleaner) scanUsers(ctx context.Context) ([]string, []string, error) {
	users, deleted, err := c.usersScanner.ScanUsers(ctx)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to discover users from bucket")
	}

	isActive := util.StringsMap(users)
	isDeleted := util.StringsMap(deleted)
	allUsers := append(users, deleted...)
	// Delete per-tenant metrics for all tenants not belonging anymore to this shard.
	// Such tenants have been moved to a different shard, so their updated metrics will
	// be exported by the new shard.
	for _, userID := range c.lastOwnedUsers {
		if !isActive[userID] && !isDeleted[userID] {
			c.tenantBlocks.DeleteLabelValues(userID)
			c.tenantBlocksMarkedForDelete.DeleteLabelValues(userID)
			c.tenantBlocksMarkedForNoCompaction.DeleteLabelValues(userID)
			c.tenantPartialBlocks.DeleteLabelValues(userID)
			c.tenantBucketIndexLastUpdate.DeleteLabelValues(userID)
			if c.cfg.ShardingStrategy == util.ShardingStrategyShuffle {
				c.remainingPlannedCompactions.DeleteLabelValues(userID)
				if c.cfg.CompactionStrategy == util.CompactionStrategyPartitioning {
					c.inProgressCompactions.DeleteLabelValues(userID)
					c.oldestPartitionGroupOffset.DeleteLabelValues(userID)
				}
			}
		}
	}
	c.lastOwnedUsers = allUsers

	return users, deleted, nil
}

func (c *BlocksCleaner) obtainVisitMarkerManager(ctx context.Context, userLogger log.Logger, userBucket objstore.InstrumentedBucket) (visitMarkerManager *VisitMarkerManager, isVisited bool, err error) {
	cleanerVisitMarker := NewCleanerVisitMarker(c.ringLifecyclerID)
	visitMarkerManager = NewVisitMarkerManager(userBucket, userLogger, c.ringLifecyclerID, cleanerVisitMarker)

	existingCleanerVisitMarker := &CleanerVisitMarker{}
	err = visitMarkerManager.ReadVisitMarker(ctx, existingCleanerVisitMarker)
	if err != nil && !errors.Is(err, errorVisitMarkerNotFound) {
		return nil, false, errors.Wrapf(err, "failed to read cleaner visit marker")
	}
	isVisited = !errors.Is(err, errorVisitMarkerNotFound) && existingCleanerVisitMarker.IsVisited(c.cleanerVisitMarkerTimeout)
	return visitMarkerManager, isVisited, nil
}

// Remove blocks and remaining data for tenant marked for deletion.
func (c *BlocksCleaner) deleteUserMarkedForDeletion(ctx context.Context, userLogger log.Logger, userBucket objstore.InstrumentedBucket, userID string) error {

	level.Info(userLogger).Log("msg", "deleting blocks for tenant marked for deletion")

	// We immediately delete the bucket index, to signal to its consumers that
	// the tenant has "no blocks" in the storage.
	if err := bucketindex.DeleteIndex(ctx, c.bucketClient, userID, c.cfgProvider); err != nil {
		return err
	}
	// Delete the bucket sync status
	if err := bucketindex.DeleteIndexSyncStatus(ctx, c.bucketClient, userID); err != nil {
		return err
	}
	c.tenantBucketIndexLastUpdate.DeleteLabelValues(userID)

	var blocksToDelete []interface{}
	err := userBucket.Iter(ctx, "", func(name string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		id, ok := block.IsBlockDir(name)
		if !ok {
			return nil
		}
		blocksToDelete = append(blocksToDelete, id)
		return nil
	})
	if err != nil {
		return err
	}

	var deletedBlocks, failed atomic.Int64
	err = concurrency.ForEach(ctx, blocksToDelete, defaultDeleteBlocksConcurrency, func(ctx context.Context, job interface{}) error {
		blockID := job.(ulid.ULID)
		err := block.Delete(ctx, userLogger, userBucket, blockID)
		if err != nil {
			failed.Add(1)
			c.blocksFailedTotal.Inc()
			level.Warn(userLogger).Log("msg", "failed to delete block", "block", blockID, "err", err)
			return nil // Continue with other blocks.
		}

		deletedBlocks.Add(1)
		c.blocksCleanedTotal.Inc()
		c.tenantBlocksCleanedTotal.WithLabelValues(userID).Inc()
		level.Info(userLogger).Log("msg", "deleted block", "block", blockID)
		return nil
	})
	if err != nil {
		return err
	}

	if failed.Load() > 0 {
		// The number of blocks left in the storage is equal to the number of blocks we failed
		// to delete. We also consider them all marked for deletion given the next run will try
		// to delete them again.
		c.tenantBlocks.WithLabelValues(userID).Set(float64(failed.Load()))
		c.tenantBlocksMarkedForDelete.WithLabelValues(userID).Set(float64(failed.Load()))
		c.tenantPartialBlocks.WithLabelValues(userID).Set(0)

		return errors.Errorf("failed to delete %d blocks", failed.Load())
	}

	// Given all blocks have been deleted, we can also remove the metrics.
	c.tenantBlocks.DeleteLabelValues(userID)
	c.tenantBlocksMarkedForDelete.DeleteLabelValues(userID)
	c.tenantBlocksMarkedForNoCompaction.DeleteLabelValues(userID)
	c.tenantPartialBlocks.DeleteLabelValues(userID)

	if deletedBlocks.Load() > 0 {
		level.Info(userLogger).Log("msg", "deleted blocks for tenant marked for deletion", "deletedBlocks", deletedBlocks.Load())
	}

	mark, err := cortex_tsdb.ReadTenantDeletionMark(ctx, c.bucketClient, userID)
	if err != nil {
		return errors.Wrap(err, "failed to read tenant deletion mark")
	}
	if mark == nil {
		return errors.Wrap(err, "cannot find tenant deletion mark anymore")
	}
	// If we have just deleted some blocks, update "finished" time. Also update "finished" time if it wasn't set yet, but there are no blocks.
	// Note: this UPDATES the tenant deletion mark. Components that use caching bucket will NOT SEE this update,
	// but that is fine -- they only check whether tenant deletion marker exists or not.
	if deletedBlocks.Load() > 0 || mark.FinishedTime == 0 {
		level.Info(userLogger).Log("msg", "updating finished time in tenant deletion mark")
		mark.FinishedTime = time.Now().Unix()
		return errors.Wrap(cortex_tsdb.WriteTenantDeletionMark(ctx, c.bucketClient, userID, mark), "failed to update tenant deletion mark")
	}
	if time.Since(time.Unix(mark.FinishedTime, 0)) < c.cfg.TenantCleanupDelay {
		return nil
	}
	level.Info(userLogger).Log("msg", "cleaning up remaining blocks data for tenant marked for deletion")
	// Let's do final cleanup of tenant.
	err = c.deleteNonDataFiles(ctx, userLogger, userBucket)
	if err != nil {
		return err
	}

	if deleted, err := bucket.DeletePrefix(ctx, userBucket, bucketindex.MarkersPathname, userLogger); err != nil {
		return errors.Wrap(err, "failed to delete marker files")
	} else if deleted > 0 {
		level.Info(userLogger).Log("msg", "deleted marker files for tenant marked for deletion", "count", deleted)
	}
	if err := cortex_tsdb.DeleteTenantDeletionMark(ctx, c.bucketClient, userID); err != nil {
		return errors.Wrap(err, "failed to delete tenant deletion mark")
	}
	return nil
}

func (c *BlocksCleaner) deleteNonDataFiles(ctx context.Context, userLogger log.Logger, userBucket objstore.InstrumentedBucket) error {
	if deleted, err := bucket.DeletePrefix(ctx, userBucket, block.DebugMetas, userLogger); err != nil {
		return errors.Wrap(err, "failed to delete "+block.DebugMetas)
	} else if deleted > 0 {
		level.Info(userLogger).Log("msg", "deleted files under "+block.DebugMetas+" for tenant marked for deletion", "count", deleted)
	}

	if c.cfg.CompactionStrategy == util.CompactionStrategyPartitioning {
		// Clean up partitioned group info files
		if deleted, err := bucket.DeletePrefix(ctx, userBucket, PartitionedGroupDirectory, userLogger); err != nil {
			return errors.Wrap(err, "failed to delete "+PartitionedGroupDirectory)
		} else if deleted > 0 {
			level.Info(userLogger).Log("msg", "deleted files under "+PartitionedGroupDirectory+" for tenant marked for deletion", "count", deleted)
		}
	}
	return nil
}

func (c *BlocksCleaner) cleanUser(ctx context.Context, userLogger log.Logger, userBucket objstore.InstrumentedBucket, userID string, firstRun bool) (returnErr error) {
	c.blocksMarkedForDeletion.WithLabelValues(userID, reasonValueRetention)
	startTime := time.Now()

	bucketIndexDeleted := false

	level.Info(userLogger).Log("msg", "started blocks cleanup and maintenance")
	defer func() {
		if returnErr != nil {
			level.Warn(userLogger).Log("msg", "failed blocks cleanup and maintenance", "err", returnErr)
		} else {
			level.Info(userLogger).Log("msg", "completed blocks cleanup and maintenance", "duration", time.Since(startTime))
		}
		c.tenantCleanDuration.WithLabelValues(userID).Set(time.Since(startTime).Seconds())
	}()

	// Migrate block deletion marks to the global markers location. This operation is a best-effort.
	if firstRun && c.cfg.BlockDeletionMarksMigrationEnabled {
		if err := bucketindex.MigrateBlockDeletionMarksToGlobalLocation(ctx, c.bucketClient, userID, c.cfgProvider); err != nil {
			level.Warn(userLogger).Log("msg", "failed to migrate block deletion marks to the global markers location", "err", err)
		} else {
			level.Info(userLogger).Log("msg", "migrated block deletion marks to the global markers location")
		}
	}

	// Reading bucket index sync stats
	idxs, err := bucketindex.ReadSyncStatus(ctx, c.bucketClient, userID, userLogger)

	if err != nil {
		level.Warn(userLogger).Log("msg", "error reading the bucket index status", "err", err)
		idxs = bucketindex.Status{Version: bucketindex.SyncStatusFileVersion, NonQueryableReason: bucketindex.Unknown}
	}

	idxs.Status = bucketindex.Ok
	idxs.SyncTime = time.Now().Unix()

	// Read the bucket index.
	begin := time.Now()
	idx, err := bucketindex.ReadIndex(ctx, c.bucketClient, userID, c.cfgProvider, c.logger)

	defer func() {
		if bucketIndexDeleted {
			level.Info(userLogger).Log("msg", "deleting bucket index sync status since bucket index is empty")
			if err := bucketindex.DeleteIndexSyncStatus(ctx, c.bucketClient, userID); err != nil {
				level.Warn(userLogger).Log("msg", "error deleting index sync status when index is empty", "err", err)
			}
			if err := c.deleteNonDataFiles(ctx, userLogger, userBucket); err != nil {
				level.Warn(userLogger).Log("msg", "error deleting non-data files", "err", err)
			}
		} else {
			bucketindex.WriteSyncStatus(ctx, c.bucketClient, userID, idxs, userLogger)
		}
	}()

	if errors.Is(err, bucketindex.ErrIndexCorrupted) {
		level.Warn(userLogger).Log("msg", "found a corrupted bucket index, recreating it")
	} else if errors.Is(err, bucket.ErrCustomerManagedKeyAccessDenied) {
		// Give up cleaning if we get access denied
		level.Warn(userLogger).Log("msg", "customer manager key access denied", "err", err)
		idxs.Status = bucketindex.CustomerManagedKeyError
		// Making the tenant non queryable until 3x the cleanup interval to give time to compactors and storegateways
		// to reload the bucket index in case the key access is re-granted
		idxs.NonQueryableUntil = time.Now().Add(3 * c.cfg.CleanupInterval).Unix()
		idxs.NonQueryableReason = bucketindex.CustomerManagedKeyError

		// Update the bucket index update time
		c.tenantBucketIndexLastUpdate.WithLabelValues(userID).SetToCurrentTime()
		return nil
	} else if err != nil && !errors.Is(err, bucketindex.ErrIndexNotFound) {
		idxs.Status = bucketindex.GenericError
		return err
	}
	level.Info(userLogger).Log("msg", "finish reading index", "duration", time.Since(begin), "duration_ms", time.Since(begin).Milliseconds())

	// Mark blocks for future deletion based on the retention period for the user.
	// Note doing this before UpdateIndex, so it reads in the deletion marks.
	// The trade-off being that retention is not applied if the index has to be
	// built, but this is rare.
	if idx != nil {
		// We do not want to stop the remaining work in the cleaner if an
		// error occurs here. Errors are logged in the function.
		retention := c.cfgProvider.CompactorBlocksRetentionPeriod(userID)
		c.applyUserRetentionPeriod(ctx, idx, retention, userBucket, userLogger, userID)
	}

	// Generate an updated in-memory version of the bucket index.
	begin = time.Now()
	w := bucketindex.NewUpdater(c.bucketClient, userID, c.cfgProvider, c.logger)
	idx, partials, totalBlocksBlocksMarkedForNoCompaction, err := w.UpdateIndex(ctx, idx)
	if err != nil {
		idxs.Status = bucketindex.GenericError
		return err
	}
	level.Info(userLogger).Log("msg", "finish updating index", "duration", time.Since(begin), "duration_ms", time.Since(begin).Milliseconds())

	// Delete blocks marked for deletion. We iterate over a copy of deletion marks because
	// we'll need to manipulate the index (removing blocks which get deleted).
	begin = time.Now()
	blocksToDelete := make([]interface{}, 0, len(idx.BlockDeletionMarks))
	var mux sync.Mutex
	for _, mark := range idx.BlockDeletionMarks.Clone() {
		if time.Since(mark.GetDeletionTime()).Seconds() <= c.cfg.DeletionDelay.Seconds() {
			continue
		}
		blocksToDelete = append(blocksToDelete, mark.ID)
	}
	level.Info(userLogger).Log("msg", "finish getting blocks to be deleted", "duration", time.Since(begin), "duration_ms", time.Since(begin).Milliseconds())

	// Concurrently deletes blocks marked for deletion, and removes blocks from index.
	begin = time.Now()
	_ = concurrency.ForEach(ctx, blocksToDelete, defaultDeleteBlocksConcurrency, func(ctx context.Context, job interface{}) error {
		blockID := job.(ulid.ULID)

		if err := block.Delete(ctx, userLogger, userBucket, blockID); err != nil {
			c.blocksFailedTotal.Inc()
			level.Warn(userLogger).Log("msg", "failed to delete block marked for deletion", "block", blockID, "err", err)
			return nil
		}

		// Remove the block from the bucket index too.
		mux.Lock()
		idx.RemoveBlock(blockID)
		mux.Unlock()

		c.blocksCleanedTotal.Inc()
		c.tenantBlocksCleanedTotal.WithLabelValues(userID).Inc()
		level.Info(userLogger).Log("msg", "deleted block marked for deletion", "block", blockID)
		return nil
	})
	level.Info(userLogger).Log("msg", "finish deleting blocks", "duration", time.Since(begin), "duration_ms", time.Since(begin).Milliseconds())

	// Partial blocks with a deletion mark can be cleaned up. This is a best effort, so we don't return
	// error if the cleanup of partial blocks fail.
	if len(partials) > 0 {
		begin = time.Now()
		c.cleanUserPartialBlocks(ctx, userID, partials, idx, userBucket, userLogger)
		level.Info(userLogger).Log("msg", "finish cleaning partial blocks", "duration", time.Since(begin), "duration_ms", time.Since(begin).Milliseconds())
	}

	if idx.IsEmpty() && len(partials) == 0 {
		level.Info(userLogger).Log("msg", "deleting bucket index since it is empty")
		if err := bucketindex.DeleteIndex(ctx, c.bucketClient, userID, c.cfgProvider); err != nil {
			return err
		}
		bucketIndexDeleted = true
	} else {
		// Upload the updated index to the storage.
		begin = time.Now()
		if err := bucketindex.WriteIndex(ctx, c.bucketClient, userID, c.cfgProvider, idx); err != nil {
			return err
		}
		level.Info(userLogger).Log("msg", "finish writing new index", "duration", time.Since(begin), "duration_ms", time.Since(begin).Milliseconds())
	}

	if c.cfg.ShardingStrategy == util.ShardingStrategyShuffle && c.cfg.CompactionStrategy == util.CompactionStrategyPartitioning {
		begin = time.Now()
		c.cleanPartitionedGroupInfo(ctx, userBucket, userLogger, userID)
		level.Info(userLogger).Log("msg", "finish cleaning partitioned group info files", "duration", time.Since(begin), "duration_ms", time.Since(begin).Milliseconds())
	}

	c.tenantBlocks.WithLabelValues(userID).Set(float64(len(idx.Blocks)))
	c.tenantBlocksMarkedForDelete.WithLabelValues(userID).Set(float64(len(idx.BlockDeletionMarks)))
	c.tenantBlocksMarkedForNoCompaction.WithLabelValues(userID).Set(float64(totalBlocksBlocksMarkedForNoCompaction))
	c.tenantBucketIndexLastUpdate.WithLabelValues(userID).SetToCurrentTime()
	c.tenantPartialBlocks.WithLabelValues(userID).Set(float64(len(partials)))
	return nil
}

func (c *BlocksCleaner) cleanPartitionedGroupInfo(ctx context.Context, userBucket objstore.InstrumentedBucket, userLogger log.Logger, userID string) {
	existentPartitionedGroupInfo := make(map[*PartitionedGroupInfo]struct {
		path   string
		status PartitionedGroupStatus
	})
	err := userBucket.Iter(ctx, PartitionedGroupDirectory, func(file string) error {
		if strings.Contains(file, PartitionVisitMarkerDirectory) {
			return nil
		}
		partitionedGroupInfo, err := ReadPartitionedGroupInfoFile(ctx, userBucket, userLogger, file)
		if err != nil {
			level.Warn(userLogger).Log("msg", "failed to read partitioned group info", "partitioned_group_info", file)
			return nil
		}

		status := partitionedGroupInfo.getPartitionedGroupStatus(ctx, userBucket, c.compactionVisitMarkerTimeout, userLogger)
		level.Debug(userLogger).Log("msg", "got partitioned group status", "partitioned_group_status", status.String())
		existentPartitionedGroupInfo[partitionedGroupInfo] = struct {
			path   string
			status PartitionedGroupStatus
		}{
			path:   file,
			status: status,
		}
		return nil
	})

	if err != nil {
		level.Warn(userLogger).Log("msg", "error return when going through partitioned group directory", "err", err)
	}

	remainingCompactions := 0
	inProgressCompactions := 0
	var oldestPartitionGroup *PartitionedGroupInfo
	defer func() {
		c.remainingPlannedCompactions.WithLabelValues(userID).Set(float64(remainingCompactions))
		c.inProgressCompactions.WithLabelValues(userID).Set(float64(inProgressCompactions))
		if c.oldestPartitionGroupOffset != nil {
			if oldestPartitionGroup != nil {
				c.oldestPartitionGroupOffset.WithLabelValues(userID).Set(float64(time.Now().Unix() - oldestPartitionGroup.CreationTime))
				level.Debug(userLogger).Log("msg", "partition group info with oldest creation time", "partitioned_group_id", oldestPartitionGroup.PartitionedGroupID, "creation_time", oldestPartitionGroup.CreationTime)
			} else {
				c.oldestPartitionGroupOffset.WithLabelValues(userID).Set(0)
			}
		}
	}()
	for partitionedGroupInfo, extraInfo := range existentPartitionedGroupInfo {
		partitionedGroupInfoFile := extraInfo.path

		remainingCompactions += extraInfo.status.PendingPartitions
		inProgressCompactions += extraInfo.status.InProgressPartitions
		if oldestPartitionGroup == nil || partitionedGroupInfo.CreationTime < oldestPartitionGroup.CreationTime {
			oldestPartitionGroup = partitionedGroupInfo
		}
		if extraInfo.status.CanDelete {
			if extraInfo.status.IsCompleted {
				// Try to remove all blocks included in partitioned group info
				if err := partitionedGroupInfo.markAllBlocksForDeletion(ctx, userBucket, userLogger, c.blocksMarkedForDeletion, userID); err != nil {
					level.Warn(userLogger).Log("msg", "unable to mark all blocks in partitioned group info for deletion", "partitioned_group_id", partitionedGroupInfo.PartitionedGroupID)
					// if one block can not be marked for deletion, we should
					// skip delete this partitioned group. next iteration
					// would try it again.
					continue
				}
			}

			if err := userBucket.Delete(ctx, partitionedGroupInfoFile); err != nil {
				level.Warn(userLogger).Log("msg", "failed to delete partitioned group info", "partitioned_group_info", partitionedGroupInfoFile, "err", err)
			} else {
				level.Info(userLogger).Log("msg", "deleted partitioned group info", "partitioned_group_info", partitionedGroupInfoFile)
			}
		}

		if extraInfo.status.CanDelete || extraInfo.status.DeleteVisitMarker {
			// Remove partition visit markers
			if _, err := bucket.DeletePrefix(ctx, userBucket, GetPartitionVisitMarkerDirectoryPath(partitionedGroupInfo.PartitionedGroupID), userLogger); err != nil {
				level.Warn(userLogger).Log("msg", "failed to delete partition visit markers for partitioned group", "partitioned_group_info", partitionedGroupInfoFile, "err", err)
			} else {
				level.Info(userLogger).Log("msg", "deleted partition visit markers for partitioned group", "partitioned_group_info", partitionedGroupInfoFile)
			}
		}
	}
}

// cleanUserPartialBlocks delete partial blocks which are safe to be deleted. The provided partials map
// and index are updated accordingly.
func (c *BlocksCleaner) cleanUserPartialBlocks(ctx context.Context, userID string, partials map[ulid.ULID]error, idx *bucketindex.Index, userBucket objstore.InstrumentedBucket, userLogger log.Logger) {
	// Collect all blocks with missing meta.json into buffered channel.
	blocks := make([]interface{}, 0, len(partials))

	for blockID, blockErr := range partials {
		// We can safely delete only blocks which are partial because the meta.json is missing.
		if !errors.Is(blockErr, bucketindex.ErrBlockMetaNotFound) {
			continue
		}
		blocks = append(blocks, blockID)
	}

	var mux sync.Mutex

	_ = concurrency.ForEach(ctx, blocks, defaultDeleteBlocksConcurrency, func(ctx context.Context, job interface{}) error {
		blockID := job.(ulid.ULID)
		// We can safely delete only partial blocks with a deletion mark.
		err := metadata.ReadMarker(ctx, userLogger, userBucket, blockID.String(), &metadata.DeletionMark{})
		if errors.Is(err, metadata.ErrorMarkerNotFound) {
			//If only visit marker exists in the block, we can safely delete it.
			isEmpty := true
			notVisitMarkerError := userBucket.ReaderWithExpectedErrs(IsNotBlockVisitMarkerError).Iter(ctx, blockID.String(), func(file string) error {
				isEmpty = false
				if !IsBlockVisitMarker(file) {
					// return error here to fail iteration fast
					// to avoid going through all files
					return ErrorNotBlockVisitMarker
				}
				return nil
			})
			if isEmpty || notVisitMarkerError != nil {
				// skip deleting partial block if block directory
				// is empty or non visit marker file exists
				return nil
			}
		} else if err != nil {
			level.Warn(userLogger).Log("msg", "error reading partial block deletion mark", "block", blockID, "err", err)
			return nil
		}

		// Hard-delete partial blocks having a deletion mark, even if the deletion threshold has not
		// been reached yet.
		if err := block.Delete(ctx, userLogger, userBucket, blockID); err != nil {
			c.blocksFailedTotal.Inc()
			level.Warn(userLogger).Log("msg", "error deleting partial block marked for deletion", "block", blockID, "err", err)
			return nil
		}

		// Remove the block from the bucket index too.
		mux.Lock()
		idx.RemoveBlock(blockID)
		delete(partials, blockID)
		mux.Unlock()

		c.blocksCleanedTotal.Inc()
		c.tenantBlocksCleanedTotal.WithLabelValues(userID).Inc()
		level.Info(userLogger).Log("msg", "deleted partial block marked for deletion", "block", blockID)
		return nil
	})
}

// applyUserRetentionPeriod marks blocks for deletion which have aged past the retention period.
func (c *BlocksCleaner) applyUserRetentionPeriod(ctx context.Context, idx *bucketindex.Index, retention time.Duration, userBucket objstore.Bucket, userLogger log.Logger, userID string) {
	// The retention period of zero is a special value indicating to never delete.
	if retention <= 0 {
		return
	}

	level.Debug(userLogger).Log("msg", "applying retention", "retention", retention.String())
	blocks := listBlocksOutsideRetentionPeriod(idx, time.Now().Add(-retention))

	// Attempt to mark all blocks. It is not critical if a marking fails, as
	// the cleaner will retry applying the retention in its next cycle.
	for _, b := range blocks {
		level.Info(userLogger).Log("msg", "applied retention: marking block for deletion", "block", b.ID, "maxTime", b.MaxTime)
		if err := block.MarkForDeletion(ctx, userLogger, userBucket, b.ID, fmt.Sprintf("block exceeding retention of %v", retention), c.blocksMarkedForDeletion.WithLabelValues(userID, reasonValueRetention)); err != nil {
			level.Warn(userLogger).Log("msg", "failed to mark block for deletion", "block", b.ID, "err", err)
		}
	}
}

// listBlocksOutsideRetentionPeriod determines the blocks which have aged past
// the specified retention period, and are not already marked for deletion.
func listBlocksOutsideRetentionPeriod(idx *bucketindex.Index, threshold time.Time) (result bucketindex.Blocks) {
	// Whilst re-marking a block is not harmful, it is wasteful and generates
	// a warning log message. Use the block deletion marks already in-memory
	// to prevent marking blocks already marked for deletion.
	marked := make(map[ulid.ULID]struct{}, len(idx.BlockDeletionMarks))
	for _, d := range idx.BlockDeletionMarks {
		marked[d.ID] = struct{}{}
	}

	for _, b := range idx.Blocks {
		maxTime := time.Unix(b.MaxTime/1000, 0)
		if maxTime.Before(threshold) {
			if _, isMarked := marked[b.ID]; !isMarked {
				result = append(result, b)
			}
		}
	}

	return
}
