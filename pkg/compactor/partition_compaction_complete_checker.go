package compactor

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"

	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
)

type PartitionCompactionBlockDeletableChecker struct {
	ctx                            context.Context
	bkt                            objstore.InstrumentedBucket
	logger                         log.Logger
	metaSyncConcurrency            int
	blocksMarkedForNoCompaction    prometheus.Counter
	blockVisitMarkerReadFailed     prometheus.Counter
	partitionedGroupInfoReadFailed prometheus.Counter
}

func NewPartitionCompactionBlockDeletableChecker(
	ctx context.Context,
	bkt objstore.InstrumentedBucket,
	logger log.Logger,
	metaSyncConcurrency int,
	blocksMarkedForNoCompaction prometheus.Counter,
	blockVisitMarkerReadFailed prometheus.Counter,
	partitionedGroupInfoReadFailed prometheus.Counter,
) *PartitionCompactionBlockDeletableChecker {
	return &PartitionCompactionBlockDeletableChecker{
		ctx:                            ctx,
		bkt:                            bkt,
		logger:                         logger,
		metaSyncConcurrency:            metaSyncConcurrency,
		blocksMarkedForNoCompaction:    blocksMarkedForNoCompaction,
		blockVisitMarkerReadFailed:     blockVisitMarkerReadFailed,
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
	if !p.isPartitionedBlockComplete(partitionedGroupInfo, currentPartitionID, blockID) {
		return false
	}
	return !p.isSampleMissing(partitionedGroupInfo)
}

func (p *PartitionCompactionBlockDeletableChecker) isPartitionedBlockComplete(partitionedGroupInfo *PartitionedGroupInfo, currentPartitionID int, blockID ulid.ULID) bool {
	partitionedGroupID := partitionedGroupInfo.PartitionedGroupID
	for _, partitionID := range partitionedGroupInfo.getPartitionIDsByBlock(blockID) {
		// Skip current partition ID since current one is completed
		if partitionID != currentPartitionID {
			blockVisitMarker, err := ReadBlockVisitMarker(p.ctx, p.bkt, p.logger, blockID.String(), partitionID, p.blockVisitMarkerReadFailed)
			if err != nil {
				level.Warn(p.logger).Log("msg", "unable to read all visit markers for block", "partitioned_group_id", partitionedGroupID, "partition_id", partitionID, "block_id", blockID, "err", err)
				return false
			}
			if !blockVisitMarker.isCompleted() {
				level.Warn(p.logger).Log("msg", "block has incomplete partition", "partitioned_group_id", partitionedGroupID, "partition_id", partitionID, "block_id", blockID)
				return false
			}
		}
	}
	level.Info(p.logger).Log("msg", "block has all partitions completed", "partitioned_group_id", partitionedGroupID, "block_id", blockID)
	return true
}

func (p *PartitionCompactionBlockDeletableChecker) isSampleMissing(partitionedGroupInfo *PartitionedGroupInfo) bool {
	allBlocks, err := p.getBlocksInTimeRange(partitionedGroupInfo.RangeStart, partitionedGroupInfo.RangeEnd)
	if err != nil {
		level.Warn(p.logger).Log("msg", "unable to get blocks in time range", "partitioned_group_id", partitionedGroupInfo.PartitionedGroupID, "range_start", partitionedGroupInfo.RangeStart, "range_end", partitionedGroupInfo.RangeEnd)
		return true
	}
	resultBlocks := p.getBlocksByPartitionedGroupID(partitionedGroupInfo, allBlocks)
	if len(resultBlocks) != partitionedGroupInfo.PartitionCount {
		level.Warn(p.logger).Log("msg", "number of result blocks does not match partition count", "partitioned_group_id", partitionedGroupInfo.PartitionedGroupID, "partition_count", partitionedGroupInfo.PartitionCount, "result_blocks", getMetaInfo(resultBlocks))
		return true
	}
	resultSamples := countSamplesFromMeta(resultBlocks)
	sourceBlocksInPartitionGroup := partitionedGroupInfo.getAllBlocks()
	sourceBlocks := make(map[ulid.ULID]*metadata.Meta)
	for _, b := range sourceBlocksInPartitionGroup {
		if _, ok := allBlocks[b]; !ok {
			level.Warn(p.logger).Log("msg", "unable to find source block", "partitioned_group_id", partitionedGroupInfo.PartitionedGroupID, "source_block_id", b.String())
			return true
		}
		sourceBlocks[b] = allBlocks[b]
	}
	sourceSamples := countSamplesFromMeta(sourceBlocks)
	if sourceSamples > resultSamples {
		sourceBlocksInfo := getMetaInfo(sourceBlocks)
		resultBlocksInfo := getMetaInfo(resultBlocks)
		level.Error(p.logger).Log("msg", "samples are missing from result blocks", "partitioned_group_id", partitionedGroupInfo.PartitionedGroupID, "result_samples", resultSamples, "source_samples", sourceSamples, "result_blocks", resultBlocksInfo, "source_blocks", sourceBlocksInfo)
		p.markBlocksNoCompact(sourceBlocksInPartitionGroup, "result-block-sample-missing", fmt.Sprintf("partitioned_group_id=%d result_blocks=%s source_blocks=%s", partitionedGroupInfo.PartitionedGroupID, resultBlocksInfo, sourceBlocksInfo))
		return true
	}
	return false
}

func (p *PartitionCompactionBlockDeletableChecker) getBlocksByPartitionedGroupID(partitionedGroupInfo *PartitionedGroupInfo, blocksMeta map[ulid.ULID]*metadata.Meta) map[ulid.ULID]*metadata.Meta {
	blocks := make(map[ulid.ULID]*metadata.Meta)
	for b, meta := range blocksMeta {
		partitionInfo, err := GetPartitionInfo(*meta)
		if err != nil {
			continue
		}
		if partitionInfo.PartitionedGroupID == partitionedGroupInfo.PartitionedGroupID {
			blocks[b] = meta
		}
	}
	return blocks
}

func (p *PartitionCompactionBlockDeletableChecker) getBlocksInTimeRange(rangeStart int64, rangeEnd int64) (map[ulid.ULID]*metadata.Meta, error) {
	idx, err := bucketindex.ReadUserIndex(p.ctx, p.bkt, p.logger)
	if err != nil {
		return nil, err
	}
	var blockIDs []ulid.ULID
	for _, b := range idx.Blocks {
		if b.MinTime >= rangeStart && b.MaxTime <= rangeEnd {
			blockIDs = append(blockIDs, b.ID)
		}
	}
	return p.getMetaByBlockIDs(blockIDs)
}

func (p *PartitionCompactionBlockDeletableChecker) getMetaByBlockIDs(blocks []ulid.ULID) (map[ulid.ULID]*metadata.Meta, error) {
	var mtx sync.Mutex
	blocksMeta := make(map[ulid.ULID]*metadata.Meta, len(blocks))
	g, _ := errgroup.WithContext(p.ctx)
	g.SetLimit(p.metaSyncConcurrency)
	for _, b := range blocks {
		select {
		case <-p.ctx.Done():
			return nil, p.ctx.Err()
		default:
		}
		g.Go(func() error {
			blockID := b
			metaReader, err := p.bkt.ReaderWithExpectedErrs(p.bkt.IsObjNotFoundErr).Get(p.ctx, path.Join(blockID.String(), metadata.MetaFilename))
			if err != nil {
				return err
			}
			meta, err := metadata.Read(metaReader)
			if err != nil {
				return err
			}
			mtx.Lock()
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

func (p *PartitionCompactionBlockDeletableChecker) markBlocksNoCompact(blocks []ulid.ULID, reason metadata.NoCompactReason, details string) error {
	g, _ := errgroup.WithContext(p.ctx)
	g.SetLimit(p.metaSyncConcurrency)
	for _, b := range blocks {
		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		default:
		}
		g.Go(func() error {
			blockID := b
			return block.MarkForNoCompact(p.ctx, p.logger, p.bkt, blockID, reason, details, p.blocksMarkedForNoCompaction)
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
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
	for b, _ := range blocks {
		ids = append(ids, b.String())
	}
	sort.Strings(ids)
	return strings.Join(ids, ",")
}
