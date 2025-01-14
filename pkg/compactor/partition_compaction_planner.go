package compactor

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/pkg/storage/tsdb"
)

var (
	plannerCompletedPartitionError = errors.New("got completed partition")
	plannerVisitedPartitionError   = errors.New("got partition visited by other compactor")
)

type PartitionCompactionPlanner struct {
	ctx                                    context.Context
	bkt                                    objstore.InstrumentedBucket
	logger                                 log.Logger
	ranges                                 []int64
	noCompBlocksFunc                       func() map[ulid.ULID]*metadata.NoCompactMark
	ringLifecyclerID                       string
	userID                                 string
	plannerDelay                           time.Duration
	partitionVisitMarkerTimeout            time.Duration
	partitionVisitMarkerFileUpdateInterval time.Duration
	compactorMetrics                       *compactorMetrics
}

func NewPartitionCompactionPlanner(
	ctx context.Context,
	bkt objstore.InstrumentedBucket,
	logger log.Logger,
	ranges []int64,
	noCompBlocksFunc func() map[ulid.ULID]*metadata.NoCompactMark,
	ringLifecyclerID string,
	userID string,
	plannerDelay time.Duration,
	partitionVisitMarkerTimeout time.Duration,
	partitionVisitMarkerFileUpdateInterval time.Duration,
	compactorMetrics *compactorMetrics,
) *PartitionCompactionPlanner {
	return &PartitionCompactionPlanner{
		ctx:                                    ctx,
		bkt:                                    bkt,
		logger:                                 logger,
		ranges:                                 ranges,
		noCompBlocksFunc:                       noCompBlocksFunc,
		ringLifecyclerID:                       ringLifecyclerID,
		userID:                                 userID,
		plannerDelay:                           plannerDelay,
		partitionVisitMarkerTimeout:            partitionVisitMarkerTimeout,
		partitionVisitMarkerFileUpdateInterval: partitionVisitMarkerFileUpdateInterval,
		compactorMetrics:                       compactorMetrics,
	}
}

func (p *PartitionCompactionPlanner) Plan(ctx context.Context, metasByMinTime []*metadata.Meta, errChan chan error, extensions any) ([]*metadata.Meta, error) {
	cortexMetaExtensions, err := tsdb.ConvertToCortexMetaExtensions(extensions)
	if err != nil {
		return nil, err
	}
	if cortexMetaExtensions == nil {
		return nil, fmt.Errorf("cortexMetaExtensions cannot be nil")
	}
	return p.PlanWithPartition(ctx, metasByMinTime, cortexMetaExtensions, errChan)
}

func (p *PartitionCompactionPlanner) PlanWithPartition(_ context.Context, metasByMinTime []*metadata.Meta, cortexMetaExtensions *tsdb.CortexMetaExtensions, errChan chan error) ([]*metadata.Meta, error) {
	partitionInfo := cortexMetaExtensions.PartitionInfo
	if partitionInfo == nil {
		return nil, fmt.Errorf("partitionInfo cannot be nil")
	}
	partitionID := partitionInfo.PartitionID
	partitionedGroupID := partitionInfo.PartitionedGroupID

	// This delay would prevent double compaction when two compactors
	// claimed same partition in grouper at same time.
	time.Sleep(p.plannerDelay)

	visitMarker := newPartitionVisitMarker(p.ringLifecyclerID, partitionedGroupID, partitionID)
	visitMarkerManager := NewVisitMarkerManager(p.bkt, p.logger, p.ringLifecyclerID, visitMarker)
	existingPartitionVisitMarker := &partitionVisitMarker{}
	err := visitMarkerManager.ReadVisitMarker(p.ctx, existingPartitionVisitMarker)
	visitMarkerExists := true
	if err != nil {
		if errors.Is(err, errorVisitMarkerNotFound) {
			visitMarkerExists = false
		} else {
			p.compactorMetrics.compactionsNotPlanned.WithLabelValues(p.userID, cortexMetaExtensions.TimeRangeStr()).Inc()
			return nil, fmt.Errorf("unable to get visit marker file for partition with partition ID %d, partitioned group ID %d: %s", partitionID, partitionedGroupID, err.Error())
		}
	}
	if visitMarkerExists {
		if existingPartitionVisitMarker.GetStatus() == Completed {
			level.Warn(p.logger).Log("msg", "partition is in completed status", "partitioned_group_id", partitionedGroupID, "partition_id", partitionID, "compactor_id", p.ringLifecyclerID, existingPartitionVisitMarker.String())
			return nil, plannerCompletedPartitionError
		}
		if !existingPartitionVisitMarker.IsPendingByCompactor(p.partitionVisitMarkerTimeout, partitionID, p.ringLifecyclerID) {
			level.Warn(p.logger).Log("msg", "partition is not visited by current compactor", "partitioned_group_id", partitionedGroupID, "partition_id", partitionID, "compactor_id", p.ringLifecyclerID, existingPartitionVisitMarker.String())
			return nil, plannerVisitedPartitionError
		}
	}

	// Ensure all blocks fits within the largest range. This is a double check
	// to ensure there's no bug in the previous blocks grouping, given this Plan()
	// is just a pass-through.
	// Modified from https://github.com/cortexproject/cortex/pull/2616/files#diff-e3051fc530c48bb276ba958dd8fadc684e546bd7964e6bc75cef9a86ef8df344R28-R63
	largestRange := p.ranges[len(p.ranges)-1]
	rangeStart := getRangeStart(metasByMinTime[0], largestRange)
	rangeEnd := rangeStart + largestRange
	noCompactMarked := p.noCompBlocksFunc()
	resultMetas := make([]*metadata.Meta, 0, len(metasByMinTime))

	for _, b := range metasByMinTime {
		if b.ULID == DUMMY_BLOCK_ID {
			continue
		}
		blockID := b.ULID.String()
		if _, excluded := noCompactMarked[b.ULID]; excluded {
			continue
		}

		if b.MinTime < rangeStart || b.MaxTime > rangeEnd {
			p.compactorMetrics.compactionsNotPlanned.WithLabelValues(p.userID, cortexMetaExtensions.TimeRangeStr()).Inc()
			level.Warn(p.logger).Log("msg", "block is outside the largest expected range", "partitioned_group_id", partitionedGroupID, "partition_id", partitionID, "block_id", blockID, "block_min_time", b.MinTime, "block_max_time", b.MaxTime, "range_start", rangeStart, "range_end", rangeEnd)
			return nil, fmt.Errorf("block %s with time range %d:%d is outside the largest expected range %d:%d", blockID, b.MinTime, b.MaxTime, rangeStart, rangeEnd)
		}

		resultMetas = append(resultMetas, b)
	}

	if len(resultMetas) < 1 {
		p.compactorMetrics.compactionsNotPlanned.WithLabelValues(p.userID, cortexMetaExtensions.TimeRangeStr()).Inc()
		level.Warn(p.logger).Log("msg", "result meta size is empty", "partitioned_group_id", partitionedGroupID, "partition_id", partitionID, "group_size", len(metasByMinTime))
		return nil, nil
	}

	go visitMarkerManager.HeartBeat(p.ctx, errChan, p.partitionVisitMarkerFileUpdateInterval, false)

	return resultMetas, nil
}
