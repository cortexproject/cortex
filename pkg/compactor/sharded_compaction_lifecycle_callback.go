package compactor

import (
	"context"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
)

type ShardedCompactionLifecycleCallback struct {
	userBucket                     objstore.InstrumentedBucket
	partitionedGroupInfoReadFailed prometheus.Counter
}

func (c ShardedCompactionLifecycleCallback) PreCompactionCallback(_ context.Context, _ log.Logger, _ *compact.Group, _ []*metadata.Meta) error {
	return nil
}

func (c ShardedCompactionLifecycleCallback) PostCompactionCallback(ctx context.Context, logger log.Logger, cg *compact.Group, blockID ulid.ULID) error {
	partitionInfo, err := ConvertToPartitionInfo(cg.Extensions())
	if err != nil {
		return err
	}
	if partitionInfo == nil {
		return nil
	}
	partitionedGroupID := partitionInfo.PartitionedGroupID
	partitionedGroupInfo, err := ReadPartitionedGroupInfo(ctx, c.userBucket, logger, partitionedGroupID, c.partitionedGroupInfoReadFailed)
	if err != nil {
		return err
	}
	// Only try to delete PartitionedGroupFile if there is only one partition.
	// For partition count greater than one, cleaner should handle the deletion.
	if partitionedGroupInfo.PartitionCount == 1 {
		partitionedGroupFile := GetPartitionedGroupFile(partitionedGroupID)
		if err := c.userBucket.Delete(ctx, partitionedGroupFile); err != nil {
			level.Warn(logger).Log("msg", "failed to delete partitioned group info", "partitioned_group_id", partitionedGroupID, "partitioned_group_info", partitionedGroupFile, "err", err)
		} else {
			level.Info(logger).Log("msg", "deleted partitioned group info", "partitioned_group_id", partitionedGroupID, "partitioned_group_info", partitionedGroupFile)
		}
	}
	return nil
}

func (c ShardedCompactionLifecycleCallback) GetBlockPopulator(_ context.Context, logger log.Logger, cg *compact.Group) (tsdb.BlockPopulator, error) {
	partitionInfo, err := ConvertToPartitionInfo(cg.Extensions())
	if err != nil {
		return nil, err
	}
	if partitionInfo == nil {
		return tsdb.DefaultBlockPopulator{}, nil
	}
	if partitionInfo.PartitionCount <= 0 {
		partitionInfo = &PartitionInfo{
			PartitionCount:     1,
			PartitionID:        partitionInfo.PartitionID,
			PartitionedGroupID: partitionInfo.PartitionedGroupID,
		}
		cg.SetExtensions(&CortexMetaExtensions{
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
