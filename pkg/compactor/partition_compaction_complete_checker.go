package compactor

import (
	"context"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/compact"
)

type PartitionCompactionBlockDeletableChecker struct {
	ctx                            context.Context
	bkt                            objstore.InstrumentedBucket
	logger                         log.Logger
	visitMarkerReadFailed          prometheus.Counter
	partitionedGroupInfoReadFailed prometheus.Counter
}

func NewPartitionCompactionBlockDeletableChecker(
	ctx context.Context,
	bkt objstore.InstrumentedBucket,
	logger log.Logger,
	visitMarkerReadFailed prometheus.Counter,
	partitionedGroupInfoReadFailed prometheus.Counter,
) *PartitionCompactionBlockDeletableChecker {
	return &PartitionCompactionBlockDeletableChecker{
		ctx:                            ctx,
		bkt:                            bkt,
		logger:                         logger,
		visitMarkerReadFailed:          visitMarkerReadFailed,
		partitionedGroupInfoReadFailed: partitionedGroupInfoReadFailed,
	}
}

func (p *PartitionCompactionBlockDeletableChecker) CanDelete(group *compact.Group, blockID ulid.ULID) bool {
	partitionInfo, err := ConvertToPartitionInfo(group.Extensions())
	if err != nil {
		return false
	}
	if partitionInfo == nil {
		return true
	}
	partitionedGroupID := partitionInfo.PartitionedGroupID
	currentPartitionID := partitionInfo.PartitionID
	partitionedGroupInfo, err := ReadPartitionedGroupInfo(p.ctx, p.bkt, p.logger, partitionedGroupID, p.partitionedGroupInfoReadFailed)
	if err != nil {
		level.Warn(p.logger).Log("msg", "unable to read partitioned group info", "partitioned_group_id", partitionedGroupID, "block_id", blockID, "err", err)
		return false
	}
	return p.isPartitionedBlockComplete(partitionedGroupInfo, currentPartitionID, blockID)
}

func (p *PartitionCompactionBlockDeletableChecker) isPartitionedBlockComplete(partitionedGroupInfo *PartitionedGroupInfo, currentPartitionID int, blockID ulid.ULID) bool {
	partitionedGroupID := partitionedGroupInfo.PartitionedGroupID
	for _, partitionID := range partitionedGroupInfo.getPartitionIDsByBlock(blockID) {
		// Skip current partition ID since current one is completed
		if partitionID != currentPartitionID {
			partitionVisitMarker, err := ReadPartitionVisitMarker(p.ctx, p.bkt, p.logger, partitionedGroupID, partitionID, p.visitMarkerReadFailed)
			if err != nil {
				level.Warn(p.logger).Log("msg", "unable to read all visit markers for partition", "partitioned_group_id", partitionedGroupID, "partition_id", partitionID, "block_id", blockID, "err", err)
				return false
			}
			if !partitionVisitMarker.isCompleted() {
				level.Warn(p.logger).Log("msg", "block has incomplete partition", "partitioned_group_id", partitionedGroupID, "partition_id", partitionID, "block_id", blockID)
				return false
			}
		}
	}
	level.Info(p.logger).Log("msg", "block has all partitions completed", "partitioned_group_id", partitionedGroupID, "block_id", blockID)
	return true
}
