package compactor

import (
	"context"
	"path/filepath"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/runutil"

	cortextsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
)

type ShardedCompactionLifecycleCallback struct {
	ctx                 context.Context
	userBucket          objstore.InstrumentedBucket
	logger              log.Logger
	metaSyncConcurrency int
	compactDir          string
	userID              string
	compactorMetrics    *compactorMetrics

	startTime time.Time
}

func NewShardedCompactionLifecycleCallback(
	ctx context.Context,
	userBucket objstore.InstrumentedBucket,
	logger log.Logger,
	metaSyncConcurrency int,
	compactDir string,
	userID string,
	compactorMetrics *compactorMetrics,
) *ShardedCompactionLifecycleCallback {
	return &ShardedCompactionLifecycleCallback{
		ctx:                 ctx,
		userBucket:          userBucket,
		logger:              logger,
		metaSyncConcurrency: metaSyncConcurrency,
		compactDir:          compactDir,
		userID:              userID,
		compactorMetrics:    compactorMetrics,
	}
}

func (c *ShardedCompactionLifecycleCallback) PreCompactionCallback(_ context.Context, logger log.Logger, g *compact.Group, meta []*metadata.Meta) error {
	c.startTime = time.Now()

	metaExt, err := cortextsdb.ConvertToCortexMetaExtensions(g.Extensions())
	if err != nil {
		level.Warn(logger).Log("msg", "unable to get cortex meta extensions", "err", err)
	} else if metaExt != nil {
		c.compactorMetrics.compactionPlanned.WithLabelValues(c.userID, metaExt.TimeRangeStr()).Inc()
	}

	// Delete local files other than current group
	var ignoreDirs []string
	for _, m := range meta {
		ignoreDirs = append(ignoreDirs, filepath.Join(g.Key(), m.ULID.String()))
	}
	if err := runutil.DeleteAll(c.compactDir, ignoreDirs...); err != nil {
		level.Warn(logger).Log("msg", "failed deleting non-current compaction group files, disk space usage might have leaked.", "err", err, "dir", c.compactDir)
	}
	return nil
}

func (c *ShardedCompactionLifecycleCallback) PostCompactionCallback(_ context.Context, logger log.Logger, cg *compact.Group, _ ulid.ULID) error {
	metaExt, err := cortextsdb.ConvertToCortexMetaExtensions(cg.Extensions())
	if err != nil {
		level.Warn(logger).Log("msg", "unable to get cortex meta extensions", "err", err)
	} else if metaExt != nil {
		c.compactorMetrics.compactionDuration.WithLabelValues(c.userID, metaExt.TimeRangeStr()).Set(time.Since(c.startTime).Seconds())
	}
	return nil
}

func (c *ShardedCompactionLifecycleCallback) GetBlockPopulator(_ context.Context, logger log.Logger, cg *compact.Group) (tsdb.BlockPopulator, error) {
	partitionInfo, err := cortextsdb.ConvertToPartitionInfo(cg.Extensions())
	if err != nil {
		return nil, err
	}
	if partitionInfo == nil {
		return tsdb.DefaultBlockPopulator{}, nil
	}
	if partitionInfo.PartitionCount <= 0 {
		partitionInfo = &cortextsdb.PartitionInfo{
			PartitionCount:               1,
			PartitionID:                  partitionInfo.PartitionID,
			PartitionedGroupID:           partitionInfo.PartitionedGroupID,
			PartitionedGroupCreationTime: partitionInfo.PartitionedGroupCreationTime,
		}
		cg.SetExtensions(&cortextsdb.CortexMetaExtensions{
			PartitionInfo: partitionInfo,
		})
	}
	populateBlockFunc := ShardedBlockPopulator{
		partitionCount: partitionInfo.PartitionCount,
		partitionID:    partitionInfo.PartitionID,
		logger:         logger,
	}
	return populateBlockFunc, nil
}
