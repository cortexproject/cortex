package compactor

import (
	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
)

type ShardedCompactionLifecycleCallback struct {
}

func (c ShardedCompactionLifecycleCallback) PreCompactionCallback(_ *compact.Group, _ []*metadata.Meta) error {
	return nil
}

func (c ShardedCompactionLifecycleCallback) PostCompactionCallback(_ *compact.Group, _ ulid.ULID) error {
	return nil
}

func (c ShardedCompactionLifecycleCallback) GetBlockPopulator(cg *compact.Group, logger log.Logger) (tsdb.BlockPopulator, error) {
	if cg.PartitionedInfo() == nil {
		return tsdb.DefaultBlockPopulator{}, nil
	}
	if cg.PartitionedInfo().PartitionCount <= 0 {
		cg.SetPartitionInfo(&metadata.PartitionInfo{
			PartitionCount:     1,
			PartitionID:        cg.PartitionedInfo().PartitionID,
			PartitionedGroupID: cg.PartitionedInfo().PartitionedGroupID,
		})
	}
	populateBlockFunc := ShardedBlockPopulator{
		partitionCount: cg.PartitionedInfo().PartitionCount,
		partitionId:    cg.PartitionedInfo().PartitionID,
		logger:         logger,
	}
	return populateBlockFunc, nil
}
