package compactor

import (
	"context"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"golang.org/x/sync/errgroup"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"

	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
)

type ShardedCompactionLifecycleCallback struct {
	ctx                            context.Context
	userBucket                     objstore.InstrumentedBucket
	logger                         log.Logger
	metaSyncConcurrency            int
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

	// Only check potential sample missing from result block here. Warning log
	// will be emitted. But no action would be taken.
	c.isSampleMissing(partitionedGroupInfo)

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

func (c ShardedCompactionLifecycleCallback) isSampleMissing(partitionedGroupInfo *PartitionedGroupInfo) bool {
	allBlocks, err := c.getBlocksInTimeRange(partitionedGroupInfo.RangeStart, partitionedGroupInfo.RangeEnd)
	if err != nil {
		level.Warn(c.logger).Log("msg", "unable to get blocks in time range", "partitioned_group_id", partitionedGroupInfo.PartitionedGroupID, "range_start", partitionedGroupInfo.RangeStart, "range_end", partitionedGroupInfo.RangeEnd)
		return true
	}
	resultBlocks := c.getBlocksByPartitionedGroupID(partitionedGroupInfo, allBlocks)
	if len(resultBlocks) != partitionedGroupInfo.PartitionCount {
		level.Warn(c.logger).Log("msg", "number of result blocks does not match partition count", "partitioned_group_id", partitionedGroupInfo.PartitionedGroupID, "partition_count", partitionedGroupInfo.PartitionCount, "result_blocks", getMetaInfo(resultBlocks))
		return true
	}
	resultSamples := countSamplesFromMeta(resultBlocks)
	sourceBlocksInPartitionGroup := partitionedGroupInfo.getAllBlocks()
	sourceBlocks := make(map[ulid.ULID]*metadata.Meta)
	for _, b := range sourceBlocksInPartitionGroup {
		if _, ok := allBlocks[b]; !ok {
			level.Warn(c.logger).Log("msg", "unable to find source block", "partitioned_group_id", partitionedGroupInfo.PartitionedGroupID, "source_block_id", b.String())
			return true
		}
		sourceBlocks[b] = allBlocks[b]
	}
	sourceSamples := countSamplesFromMeta(sourceBlocks)
	if sourceSamples > resultSamples {
		sourceBlocksInfo := getMetaInfo(sourceBlocks)
		resultBlocksInfo := getMetaInfo(resultBlocks)
		level.Warn(c.logger).Log("msg", "samples are missing from result blocks", "partitioned_group_id", partitionedGroupInfo.PartitionedGroupID, "result_samples", resultSamples, "source_samples", sourceSamples, "result_blocks", resultBlocksInfo, "source_blocks", sourceBlocksInfo)
		return true
	}
	level.Info(c.logger).Log("msg", "samples check complete successfully", "partitioned_group_id", partitionedGroupInfo.PartitionedGroupID, "result_samples", resultSamples, "source_samples", sourceSamples)
	return false
}

func (c ShardedCompactionLifecycleCallback) getBlocksByPartitionedGroupID(partitionedGroupInfo *PartitionedGroupInfo, blocksMeta map[ulid.ULID]*metadata.Meta) map[ulid.ULID]*metadata.Meta {
	blocks := make(map[ulid.ULID]*metadata.Meta)
	for b, meta := range blocksMeta {
		partitionInfo, err := GetPartitionInfo(*meta)
		if partitionInfo == nil || err != nil {
			level.Debug(c.logger).Log("msg", "unable to get partition info for block", "block", b.String(), "meta", *meta)
			continue
		}
		if partitionInfo.PartitionedGroupID == partitionedGroupInfo.PartitionedGroupID {
			blocks[b] = meta
		}
	}
	return blocks
}

func (c ShardedCompactionLifecycleCallback) getBlocksInTimeRange(rangeStart int64, rangeEnd int64) (map[ulid.ULID]*metadata.Meta, error) {
	discovered := make(map[ulid.ULID]struct{})
	idx, err := bucketindex.ReadUserIndex(c.ctx, c.userBucket, c.logger)
	if err != nil {
		return nil, err
	}
	var blockIDs []ulid.ULID
	for _, b := range idx.Blocks {
		discovered[b.ID] = struct{}{}
		if b.MinTime >= rangeStart && b.MaxTime <= rangeEnd {
			blockIDs = append(blockIDs, b.ID)
		}
	}
	err = c.userBucket.Iter(c.ctx, "", func(name string) error {
		if id, ok := block.IsBlockDir(name); ok {
			if _, ok := discovered[id]; !ok {
				blockIDs = append(blockIDs, id)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	blocks, err := c.getMetaByBlockIDs(blockIDs)
	if err != nil {
		return nil, err
	}
	result := make(map[ulid.ULID]*metadata.Meta)
	for id, b := range blocks {
		if b.MinTime >= rangeStart && b.MaxTime <= rangeEnd {
			level.Debug(c.logger).Log("msg", "found block in time range", "range_start", rangeStart, "range_end", rangeEnd, "block", id.String(), "block_range_start", b.MinTime, "block_range_end", b.MaxTime)
			result[id] = b
		}
	}
	return result, nil
}

func (c ShardedCompactionLifecycleCallback) getMetaByBlockIDs(blocks []ulid.ULID) (map[ulid.ULID]*metadata.Meta, error) {
	var mtx sync.Mutex
	blocksMeta := make(map[ulid.ULID]*metadata.Meta, len(blocks))
	g, _ := errgroup.WithContext(c.ctx)
	g.SetLimit(c.metaSyncConcurrency)
	for _, b := range blocks {
		select {
		case <-c.ctx.Done():
			return nil, c.ctx.Err()
		default:
		}
		blockID := b
		g.Go(func() error {
			metaReader, err := c.userBucket.ReaderWithExpectedErrs(c.userBucket.IsObjNotFoundErr).Get(c.ctx, path.Join(blockID.String(), metadata.MetaFilename))
			if err != nil {
				return err
			}
			meta, err := metadata.Read(metaReader)
			if err != nil {
				return err
			}
			mtx.Lock()
			level.Debug(c.logger).Log("msg", "read metadata for block", "block", blockID, "meta", *meta)
			blocksMeta[blockID] = meta
			mtx.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return blocksMeta, nil
}

func countSamplesFromMeta(blocks map[ulid.ULID]*metadata.Meta) uint64 {
	samples := uint64(0)
	for _, b := range blocks {
		samples += b.Stats.NumSamples
	}
	return samples
}

func getMetaInfo(blocks map[ulid.ULID]*metadata.Meta) string {
	var ids []string
	for b := range blocks {
		ids = append(ids, b.String())
	}
	sort.Strings(ids)
	return strings.Join(ids, ",")
}
