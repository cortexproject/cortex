package storegateway

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
)

type MetadataFilterWithBucketIndex interface {
	// FilterWithBucketIndex is like Thanos MetadataFilter.Filter() but it provides in input the bucket index too.
	FilterWithBucketIndex(ctx context.Context, metas map[ulid.ULID]*metadata.Meta, idx *bucketindex.Index, synced block.GaugeVec) error
}

// IgnoreDeletionMarkFilter is like the Thanos IgnoreDeletionMarkFilter, but it also implements
// the MetadataFilterWithBucketIndex interface.
type IgnoreDeletionMarkFilter struct {
	upstream *block.IgnoreDeletionMarkFilter

	delay           time.Duration
	deletionMarkMap map[ulid.ULID]*metadata.DeletionMark
}

// NewIgnoreDeletionMarkFilter creates IgnoreDeletionMarkFilter.
func NewIgnoreDeletionMarkFilter(logger log.Logger, bkt objstore.InstrumentedBucketReader, delay time.Duration, concurrency int) *IgnoreDeletionMarkFilter {
	return &IgnoreDeletionMarkFilter{
		upstream: block.NewIgnoreDeletionMarkFilter(logger, bkt, delay, concurrency),
		delay:    delay,
	}
}

// DeletionMarkBlocks returns blocks that were marked for deletion.
func (f *IgnoreDeletionMarkFilter) DeletionMarkBlocks() map[ulid.ULID]*metadata.DeletionMark {
	// If the cached deletion marks exist it means the filter function was called with the bucket
	// index, so it's safe to return it.
	if f.deletionMarkMap != nil {
		return f.deletionMarkMap
	}

	return f.upstream.DeletionMarkBlocks()
}

// Filter implements block.MetadataFilter.
func (f *IgnoreDeletionMarkFilter) Filter(ctx context.Context, metas map[ulid.ULID]*metadata.Meta, synced block.GaugeVec, modified block.GaugeVec) error {
	return f.upstream.Filter(ctx, metas, synced, modified)
}

// FilterWithBucketIndex implements MetadataFilterWithBucketIndex.
func (f *IgnoreDeletionMarkFilter) FilterWithBucketIndex(_ context.Context, metas map[ulid.ULID]*metadata.Meta, idx *bucketindex.Index, synced block.GaugeVec) error {
	// Build a map of block deletion marks
	marks := make(map[ulid.ULID]*metadata.DeletionMark, len(idx.BlockDeletionMarks))
	for _, mark := range idx.BlockDeletionMarks {
		marks[mark.ID] = mark.ThanosDeletionMark()
	}

	// Keep it cached.
	f.deletionMarkMap = marks

	for _, mark := range marks {
		if _, ok := metas[mark.ID]; !ok {
			continue
		}

		if time.Since(time.Unix(mark.DeletionTime, 0)).Seconds() > f.delay.Seconds() {
			synced.WithLabelValues(block.MarkedForDeletionMeta).Inc()
			delete(metas, mark.ID)
		}
	}

	return nil
}

// IgnoreParquetBlocksFilter drops Parquet-converted blocks from the TSDB store
// sync so they are served by the Parquet store instead.
//
// It also records the IDs of all Parquet-converted blocks in the bucket index and
// exposes them via ParquetBlockIDs. The hybrid bucket store routes a block to the
// Parquet store iff it is recorded here, so excluding a block from the TSDB store and
// routing it to the Parquet store always use the same bucket index snapshot.
type IgnoreParquetBlocksFilter struct {
	logger log.Logger

	// parquetBlockIDs is the set of Parquet-converted block IDs in the bucket index as of the
	// last sync, including blocks this store-gateway does not own.
	parquetBlockIDsMu sync.RWMutex
	parquetBlockIDs   map[string]struct{}
}

func NewIgnoreParquetBlocksFilter(logger log.Logger) *IgnoreParquetBlocksFilter {
	return &IgnoreParquetBlocksFilter{logger: logger}
}

// Filter implements block.MetadataFilter.
//
// Without the bucket index we cannot tell whether a block has been converted to
// Parquet, so this is intentionally a no-op.
func (f *IgnoreParquetBlocksFilter) Filter(_ context.Context, _ map[ulid.ULID]*metadata.Meta, _ block.GaugeVec, _ block.GaugeVec) error {
	return nil
}

// FilterWithBucketIndex implements MetadataFilterWithBucketIndex.
func (f *IgnoreParquetBlocksFilter) FilterWithBucketIndex(_ context.Context, metas map[ulid.ULID]*metadata.Meta, idx *bucketindex.Index, synced block.GaugeVec) error {
	parquetBlockIDs := make(map[string]struct{})
	for _, b := range idx.ParquetBlocks() {
		if _, ok := metas[b.ID]; ok {
			level.Debug(f.logger).Log("msg", "ignoring block because it has been converted to parquet", "block", b.ID)
			synced.WithLabelValues(parquetConvertedMeta).Inc()
			delete(metas, b.ID)
		}

		// Record every Parquet block for routing, even one already excluded by a previous filter.
		parquetBlockIDs[b.ID.String()] = struct{}{}
	}

	f.parquetBlockIDsMu.Lock()
	f.parquetBlockIDs = parquetBlockIDs
	f.parquetBlockIDsMu.Unlock()

	return nil
}

// ParquetBlockIDs returns the Parquet-converted block IDs as of the last sync, or nil
// if the filter has not run yet.
func (f *IgnoreParquetBlocksFilter) ParquetBlockIDs() map[string]struct{} {
	f.parquetBlockIDsMu.RLock()
	defer f.parquetBlockIDsMu.RUnlock()
	return f.parquetBlockIDs
}

func NewIgnoreNonQueryableBlocksFilter(logger log.Logger, ignoreWithin time.Duration) *IgnoreNonQueryableBlocksFilter {
	return &IgnoreNonQueryableBlocksFilter{
		logger:       logger,
		ignoreWithin: ignoreWithin,
	}
}

// IgnoreNonQueryableBlocksFilter ignores blocks that are too new be queried.
// This has be used in conjunction with `-querier.query-store-after` with some buffer.
type IgnoreNonQueryableBlocksFilter struct {
	// Blocks that were created since `now() - ignoreWithin` will not be synced.
	ignoreWithin time.Duration
	logger       log.Logger
}

// Filter implements block.MetadataFilter.
func (f *IgnoreNonQueryableBlocksFilter) Filter(ctx context.Context, metas map[ulid.ULID]*metadata.Meta, synced block.GaugeVec, modified block.GaugeVec) error {
	ignoreWithin := time.Now().Add(-f.ignoreWithin).UnixMilli()

	for id, m := range metas {
		if m.MinTime > ignoreWithin {
			level.Debug(f.logger).Log("msg", "ignoring block because it won't be queried", "id", id)
			delete(metas, id)
		}
	}

	return nil
}
