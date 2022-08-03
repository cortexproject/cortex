package compactor

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
)

type ShuffleShardingPlanner struct {
	ctx                 context.Context
	bkt                 objstore.Bucket
	logger              log.Logger
	ranges              []int64
	noCompBlocksFunc    func() map[ulid.ULID]*metadata.NoCompactMark
	ringLifecyclerID    string
	blockLockTimeout    time.Duration
	blockLockReadFailed prometheus.Counter
}

func NewShuffleShardingPlanner(
	ctx context.Context,
	bkt objstore.Bucket,
	logger log.Logger,
	ranges []int64,
	noCompBlocksFunc func() map[ulid.ULID]*metadata.NoCompactMark,
	ringLifecyclerID string,
	blockLockTimeout time.Duration,
	blockLockReadFailed prometheus.Counter,
) *ShuffleShardingPlanner {
	return &ShuffleShardingPlanner{
		ctx:                 ctx,
		bkt:                 bkt,
		logger:              logger,
		ranges:              ranges,
		noCompBlocksFunc:    noCompBlocksFunc,
		ringLifecyclerID:    ringLifecyclerID,
		blockLockTimeout:    blockLockTimeout,
		blockLockReadFailed: blockLockReadFailed,
	}
}

func (p *ShuffleShardingPlanner) Plan(_ context.Context, metasByMinTime []*metadata.Meta) ([]*metadata.Meta, error) {
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

		blockLocker, err := ReadBlockLocker(p.ctx, p.bkt, blockID, p.blockLockReadFailed)
		if err != nil {
			if errors.Is(err, ErrorBlockLockNotFound) {
				resultMetas = append(resultMetas, b)
				continue
			}
			return nil, fmt.Errorf("unable to get lock file for block %s: %s", blockID, err.Error())
		}
		if blockLocker.isLockedForCompactor(p.blockLockTimeout, p.ringLifecyclerID) {
			return nil, fmt.Errorf("block %s is locked for compactor %s. but current compactor is %s", blockID, blockLocker.CompactorID, p.ringLifecyclerID)
		}

		resultMetas = append(resultMetas, b)
	}

	if len(resultMetas) < 2 {
		return nil, nil
	}

	return resultMetas, nil
}
