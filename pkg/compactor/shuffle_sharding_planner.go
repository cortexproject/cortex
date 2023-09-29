package compactor

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

type ShuffleShardingPlanner struct {
	ctx                           context.Context
	bkt                           objstore.InstrumentedBucket
	logger                        log.Logger
	ranges                        []int64
	noCompBlocksFunc              func() map[ulid.ULID]*metadata.NoCompactMark
	ringLifecyclerID              string
	visitMarkerTimeout            time.Duration
	visitMarkerFileUpdateInterval time.Duration
	visitMarkerReadFailed         prometheus.Counter
	visitMarkerWriteFailed        prometheus.Counter
}

func NewShuffleShardingPlanner(
	ctx context.Context,
	bkt objstore.InstrumentedBucket,
	logger log.Logger,
	ranges []int64,
	noCompBlocksFunc func() map[ulid.ULID]*metadata.NoCompactMark,
	ringLifecyclerID string,
	visitMarkerTimeout time.Duration,
	visitMarkerFileUpdateInterval time.Duration,
	visitMarkerReadFailed prometheus.Counter,
	visitMarkerWriteFailed prometheus.Counter,
) *ShuffleShardingPlanner {
	return &ShuffleShardingPlanner{
		ctx:                           ctx,
		bkt:                           bkt,
		logger:                        logger,
		ranges:                        ranges,
		noCompBlocksFunc:              noCompBlocksFunc,
		ringLifecyclerID:              ringLifecyclerID,
		visitMarkerTimeout:            visitMarkerTimeout,
		visitMarkerFileUpdateInterval: visitMarkerFileUpdateInterval,
		visitMarkerReadFailed:         visitMarkerReadFailed,
		visitMarkerWriteFailed:        visitMarkerWriteFailed,
	}
}

func (p *ShuffleShardingPlanner) Plan(ctx context.Context, metasByMinTime []*metadata.Meta, errChan chan error, extensions any) ([]*metadata.Meta, error) {
	partitionInfo, err := ConvertToPartitionInfo(extensions)
	if err != nil {
		return nil, err
	}
	if partitionInfo == nil {
		return nil, fmt.Errorf("partitionInfo cannot be nil")
	}
	return p.PlanWithPartition(ctx, metasByMinTime, partitionInfo.PartitionedGroupID, partitionInfo.PartitionID, errChan)
}

func (p *ShuffleShardingPlanner) PlanWithPartition(_ context.Context, metasByMinTime []*metadata.Meta, partitionedGroupID uint32, partitionID int, errChan chan error) ([]*metadata.Meta, error) {
	partitionVisitMarker, err := ReadPartitionVisitMarker(p.ctx, p.bkt, p.logger, partitionedGroupID, partitionID, p.visitMarkerReadFailed)
	if err != nil {
		// shuffle_sharding_grouper should put visit marker file for blocks ready for
		// compaction. So error should be returned if visit marker file does not exist.
		return nil, fmt.Errorf("unable to get visit marker file for partition with partition ID %d, partitioned group ID %d: %s", partitionID, partitionedGroupID, err.Error())
	}
	if partitionVisitMarker.isCompleted() {
		return nil, fmt.Errorf("partition with partition ID %d is in completed status", partitionID)
	}
	if !partitionVisitMarker.isVisitedByCompactor(p.visitMarkerTimeout, partitionID, p.ringLifecyclerID) {
		level.Warn(p.logger).Log("msg", "partition is not visited by current compactor", "partitioned_group_id", partitionedGroupID, "partition_id", partitionID, "compactor_id", p.ringLifecyclerID,
			"marker_partitioned_group_id", partitionVisitMarker.PartitionedGroupID, "marker_partition_id", partitionVisitMarker.PartitionID, "marker_compactor_id", partitionVisitMarker.CompactorID, "marker_visit_time", partitionVisitMarker.VisitTime)
		return nil, nil
	}

	// Ensure all blocks fits within the largest range. This is a double check
	// to ensure there's no bug in the previous blocks grouping, given this Plan()
	// is just a pass-through.
	// Modifed from https://github.com/cortexproject/cortex/pull/2616/files#diff-e3051fc530c48bb276ba958dd8fadc684e546bd7964e6bc75cef9a86ef8df344R28-R63
	largestRange := p.ranges[len(p.ranges)-1]
	rangeStart := getRangeStart(metasByMinTime[0], largestRange)
	rangeEnd := rangeStart + largestRange
	noCompactMarked := p.noCompBlocksFunc()
	resultMetas := make([]*metadata.Meta, 0, len(metasByMinTime))

	for _, b := range metasByMinTime {
		blockID := b.ULID.String()
		if _, excluded := noCompactMarked[b.ULID]; excluded {
			continue
		}

		if b.MinTime < rangeStart || b.MaxTime > rangeEnd {
			return nil, fmt.Errorf("block %s with time range %d:%d is outside the largest expected range %d:%d", blockID, b.MinTime, b.MaxTime, rangeStart, rangeEnd)
		}

		resultMetas = append(resultMetas, b)
	}

	if len(resultMetas) < 2 {
		level.Info(p.logger).Log("msg", "result meta size is less than 2", "partitioned_group_id", partitionedGroupID, "partition_id", partitionID, "size", len(resultMetas))
		return nil, nil
	}

	go markPartitionVisitedHeartBeat(p.ctx, p.bkt, p.logger, partitionedGroupID, partitionID, p.ringLifecyclerID, p.visitMarkerFileUpdateInterval, p.visitMarkerWriteFailed, errChan)

	return resultMetas, nil
}
