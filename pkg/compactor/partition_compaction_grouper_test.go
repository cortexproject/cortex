package compactor

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	thanosblock "github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortextsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

var (
	M = time.Minute.Milliseconds()
	H = time.Hour.Milliseconds()
)

func TestPartitionCompactionGrouper_GenerateCompactionJobs(t *testing.T) {
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	block3 := ulid.MustNew(3, nil)
	block4 := ulid.MustNew(4, nil)
	block5 := ulid.MustNew(5, nil)
	block6 := ulid.MustNew(6, nil)
	block7 := ulid.MustNew(7, nil)

	testCompactorID := "test-compactor"
	//otherCompactorID := "other-compactor"

	userID := "test-user"
	partitionedGroupID_0_2 := hashGroup(userID, 0*H, 2*H)
	partitionedGroupID_0_12 := hashGroup(userID, 0*H, 12*H)
	partitionedGroupID_0_24 := hashGroup(userID, 0*H, 24*H)

	tests := map[string]generateCompactionJobsTestCase{
		"only level 1 blocks": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			existingPartitionedGroups: []mockExistingPartitionedGroup{},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 2 * H},
				{blocks: []ulid.ULID{block3, block4}, partitionCount: 1, partitionID: 0, rangeStart: 2 * H, rangeEnd: 4 * H},
			},
		},
		"only level 1 blocks with ingestion replication factor 3": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}, Stats: tsdb.BlockStats{NumSeries: 1}},
						Thanos:    metadata.Thanos{Files: []metadata.File{{RelPath: thanosblock.IndexFilename, SizeBytes: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}, Stats: tsdb.BlockStats{NumSeries: 1}},
						Thanos:    metadata.Thanos{Files: []metadata.File{{RelPath: thanosblock.IndexFilename, SizeBytes: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}, Stats: tsdb.BlockStats{NumSeries: 1}},
						Thanos:    metadata.Thanos{Files: []metadata.File{{RelPath: thanosblock.IndexFilename, SizeBytes: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}, Stats: tsdb.BlockStats{NumSeries: 1}},
						Thanos:    metadata.Thanos{Files: []metadata.File{{RelPath: thanosblock.IndexFilename, SizeBytes: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block5: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}, Stats: tsdb.BlockStats{NumSeries: 1}},
						Thanos:    metadata.Thanos{Files: []metadata.File{{RelPath: thanosblock.IndexFilename, SizeBytes: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block6: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block6, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}, Stats: tsdb.BlockStats{NumSeries: 1}},
						Thanos:    metadata.Thanos{Files: []metadata.File{{RelPath: thanosblock.IndexFilename, SizeBytes: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			existingPartitionedGroups: []mockExistingPartitionedGroup{},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2, block3, block4, block5, block6}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 2 * H},
			},
			ingestionReplicationFactor: 3,
		},
		"only level 1 blocks, there is existing partitioned group file": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			existingPartitionedGroups: []mockExistingPartitionedGroup{
				{rangeStart: 0 * H, rangeEnd: 2 * H, partitionCount: 1, partitions: []Partition{
					{PartitionID: 0, Blocks: []ulid.ULID{block1, block2}},
				}},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 2 * H},
				{blocks: []ulid.ULID{block3, block4}, partitionCount: 1, partitionID: 0, rangeStart: 2 * H, rangeEnd: 4 * H},
			},
		},
		"only level 1 blocks, there are existing partitioned group files for all blocks": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			existingPartitionedGroups: []mockExistingPartitionedGroup{
				{rangeStart: 0 * H, rangeEnd: 2 * H, partitionCount: 1, partitions: []Partition{
					{PartitionID: 0, Blocks: []ulid.ULID{block1, block2}},
				}},
				{rangeStart: 2 * H, rangeEnd: 4 * H, partitionCount: 1, partitions: []Partition{
					{PartitionID: 0, Blocks: []ulid.ULID{block3, block4}},
				}},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 2 * H},
				{blocks: []ulid.ULID{block3, block4}, partitionCount: 1, partitionID: 0, rangeStart: 2 * H, rangeEnd: 4 * H},
			},
		},
		"only level 2 blocks": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block5: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 12 * H, MaxTime: 14 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			existingPartitionedGroups: []mockExistingPartitionedGroup{},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2, block3, block4}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 12 * H},
			},
		},
		"only level 2 blocks, there is existing partitioned group file": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block5: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 12 * H, MaxTime: 14 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			existingPartitionedGroups: []mockExistingPartitionedGroup{
				{rangeStart: 0 * H, rangeEnd: 12 * H, partitionCount: 1, partitions: []Partition{
					{PartitionID: 0, Blocks: []ulid.ULID{block1, block2, block3, block4}},
				}},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2, block3, block4}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 12 * H},
			},
		},
		"only level 2 blocks from same time range": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			existingPartitionedGroups: []mockExistingPartitionedGroup{},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 12 * H},
			},
		},
		"mix level 1 and level 2 blocks": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block5: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 10 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			existingPartitionedGroups: []mockExistingPartitionedGroup{},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block4, block5}, partitionCount: 1, partitionID: 0, rangeStart: 10 * H, rangeEnd: 12 * H},
			},
		},
		"mix level 1 and level 2 blocks, there is partitioned group file for level 1 blocks": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block5: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 10 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			existingPartitionedGroups: []mockExistingPartitionedGroup{
				{rangeStart: 10 * H, rangeEnd: 12 * H, partitionCount: 1, partitions: []Partition{
					{PartitionID: 0, Blocks: []ulid.ULID{block4, block5}},
				}},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block4, block5}, partitionCount: 1, partitionID: 0, rangeStart: 10 * H, rangeEnd: 12 * H},
			},
		},
		"mix level 1 and level 2 blocks in different time range": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 12 * H, MaxTime: 14 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block5: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 12 * H, MaxTime: 14 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			existingPartitionedGroups: []mockExistingPartitionedGroup{},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block4, block5}, partitionCount: 1, partitionID: 0, rangeStart: 12 * H, rangeEnd: 14 * H},
				{blocks: []ulid.ULID{block1, block2, block3}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 12 * H},
			},
		},
		"mix level 1 and level 2 blocks in different time range, there are partitioned group files for all groups": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 12 * H, MaxTime: 14 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block5: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 12 * H, MaxTime: 14 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			existingPartitionedGroups: []mockExistingPartitionedGroup{
				{rangeStart: 0 * H, rangeEnd: 12 * H, partitionCount: 1, partitions: []Partition{
					{PartitionID: 0, Blocks: []ulid.ULID{block1, block2, block3}},
				}},
				{rangeStart: 12 * H, rangeEnd: 14 * H, partitionCount: 1, partitions: []Partition{
					{PartitionID: 0, Blocks: []ulid.ULID{block4, block5}},
				}},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block4, block5}, partitionCount: 1, partitionID: 0, rangeStart: 12 * H, rangeEnd: 14 * H},
				{blocks: []ulid.ULID{block1, block2, block3}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 12 * H},
			},
		},
		"level 2 blocks with ingestion replication factor 3": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}, Stats: tsdb.BlockStats{NumSeries: 1}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}, Files: []metadata.File{{RelPath: thanosblock.IndexFilename, SizeBytes: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}, Stats: tsdb.BlockStats{NumSeries: 1}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}, Files: []metadata.File{{RelPath: thanosblock.IndexFilename, SizeBytes: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}, Stats: tsdb.BlockStats{NumSeries: 1}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}, Files: []metadata.File{{RelPath: thanosblock.IndexFilename, SizeBytes: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}, Stats: tsdb.BlockStats{NumSeries: 1}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}, Files: []metadata.File{{RelPath: thanosblock.IndexFilename, SizeBytes: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block5: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 4 * H, MaxTime: 6 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}, Stats: tsdb.BlockStats{NumSeries: 1}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}, Files: []metadata.File{{RelPath: thanosblock.IndexFilename, SizeBytes: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block6: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block6, MinTime: 4 * H, MaxTime: 6 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}, Stats: tsdb.BlockStats{NumSeries: 1}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}, Files: []metadata.File{{RelPath: thanosblock.IndexFilename, SizeBytes: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			existingPartitionedGroups: []mockExistingPartitionedGroup{},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block3, block5}, partitionCount: 2, partitionID: 0, rangeStart: 0 * H, rangeEnd: 12 * H},
				{blocks: []ulid.ULID{block2, block4, block6}, partitionCount: 2, partitionID: 1, rangeStart: 0 * H, rangeEnd: 12 * H},
			},
			ingestionReplicationFactor: 3,
		},
		"level 2 blocks along with level 3 blocks from some of partitions, level 1 blocks in different time range, there are partitioned group files for all groups": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 0 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 4, PartitionID: 0}}},
					},
					timeRange:        12 * time.Hour,
					hasNoCompactMark: false,
				},
				block5: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 0 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 4, PartitionID: 2}}},
					},
					timeRange:        12 * time.Hour,
					hasNoCompactMark: false,
				},
				block6: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block6, MinTime: 22 * H, MaxTime: 24 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block7: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block7, MinTime: 22 * H, MaxTime: 24 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			existingPartitionedGroups: []mockExistingPartitionedGroup{
				{rangeStart: 0 * H, rangeEnd: 12 * H, partitionCount: 4, partitions: []Partition{
					{PartitionID: 0, Blocks: []ulid.ULID{block1, block3}},
					{PartitionID: 1, Blocks: []ulid.ULID{block2, block3}},
					{PartitionID: 2, Blocks: []ulid.ULID{block1, block3}},
					{PartitionID: 3, Blocks: []ulid.ULID{block2, block3}},
				}, partitionVisitMarkers: map[int]mockPartitionVisitMarker{
					0: {partitionID: 0, compactorID: testCompactorID, isExpired: true, status: Completed},
					2: {partitionID: 2, compactorID: testCompactorID, isExpired: false, status: Completed},
				}},
				{rangeStart: 22 * H, rangeEnd: 24 * H, partitionCount: 1, partitions: []Partition{
					{PartitionID: 0, Blocks: []ulid.ULID{block6, block7}},
				}},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block6, block7}, partitionCount: 1, partitionID: 0, rangeStart: 22 * H, rangeEnd: 24 * H},
				{blocks: []ulid.ULID{block1, block3}, partitionCount: 4, partitionID: 0, rangeStart: 0 * H, rangeEnd: 12 * H},
				{blocks: []ulid.ULID{block2, block3}, partitionCount: 4, partitionID: 1, rangeStart: 0 * H, rangeEnd: 12 * H},
				{blocks: []ulid.ULID{block1, block3}, partitionCount: 4, partitionID: 2, rangeStart: 0 * H, rangeEnd: 12 * H},
				{blocks: []ulid.ULID{block2, block3}, partitionCount: 4, partitionID: 3, rangeStart: 0 * H, rangeEnd: 12 * H},
			},
		},
		"level 2 blocks in first 12h are all complete, level 2 blocks in second 12h have not started compaction, there is no partitioned group file": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 4 * H, MaxTime: 6 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 6 * H, MaxTime: 8 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block5: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 10 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block6: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block6, MinTime: 12 * H, MaxTime: 14 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block7: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block7, MinTime: 12 * H, MaxTime: 14 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			existingPartitionedGroups: []mockExistingPartitionedGroup{},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block6, block7}, partitionCount: 1, partitionID: 0, rangeStart: 12 * H, rangeEnd: 14 * H},
				{blocks: []ulid.ULID{block1, block2, block3, block4, block5}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 12 * H},
			},
		},
		"level 2 blocks are all complete, there is no partitioned group file": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 4 * H, MaxTime: 6 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block5: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 10 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block6: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block6, MinTime: 12 * H, MaxTime: 14 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block7: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block7, MinTime: 14 * H, MaxTime: 16 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			existingPartitionedGroups: []mockExistingPartitionedGroup{},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2, block3, block4, block5}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 12 * H},
				{blocks: []ulid.ULID{block6, block7}, partitionCount: 1, partitionID: 0, rangeStart: 12 * H, rangeEnd: 24 * H},
			},
		},
		"level 2 blocks are complete only in second half of 12h, there is existing partitioned group file": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 4 * H, MaxTime: 6 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block5: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 0 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        12 * time.Hour,
					hasNoCompactMark: false,
				},
				block6: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block6, MinTime: 12 * H, MaxTime: 24 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        12 * time.Hour,
					hasNoCompactMark: false,
				},
				block7: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block7, MinTime: 12 * H, MaxTime: 24 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        12 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			existingPartitionedGroups: []mockExistingPartitionedGroup{
				{rangeStart: 0 * H, rangeEnd: 12 * H, partitionCount: 2, partitions: []Partition{
					{PartitionID: 0, Blocks: []ulid.ULID{block1, block2, block3, block4}},
					{PartitionID: 1, Blocks: []ulid.ULID{block1, block2, block3, block4}},
				}, partitionVisitMarkers: map[int]mockPartitionVisitMarker{
					1: {partitionID: 1, compactorID: testCompactorID, isExpired: true, status: Completed},
				}},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2, block3, block4}, partitionCount: 2, partitionID: 0, rangeStart: 0 * H, rangeEnd: 12 * H},
				{blocks: []ulid.ULID{block1, block2, block3, block4}, partitionCount: 2, partitionID: 1, rangeStart: 0 * H, rangeEnd: 12 * H},
			},
		},
		"level 3 blocks are complete, there are some level 2 blocks not deleted, there is existing partitioned group file": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 4 * H, MaxTime: 6 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 10 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block5: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 0 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        12 * time.Hour,
					hasNoCompactMark: false,
				},
				block6: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block6, MinTime: 12 * H, MaxTime: 24 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        12 * time.Hour,
					hasNoCompactMark: false,
				},
				block7: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block7, MinTime: 12 * H, MaxTime: 24 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        12 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			existingPartitionedGroups: []mockExistingPartitionedGroup{
				{rangeStart: 0 * H, rangeEnd: 12 * H, partitionCount: 1, partitions: []Partition{
					{PartitionID: 0, Blocks: []ulid.ULID{block1, block2, block3, block4}},
				}, partitionVisitMarkers: map[int]mockPartitionVisitMarker{
					0: {partitionID: 0, compactorID: testCompactorID, isExpired: true, status: Completed},
				}},
			},
			expected: []expectedCompactionJob{
				// nothing should be grouped. cleaner should mark all level 2 blocks for deletion
				// and delete partitioned group file since level 2 to level 3 compaction is complete
			},
		},
		"recompact one level 1 block with level 2 blocks": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2, block3}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 2 * H},
			},
		},
		"recompact one level 1 block with level 2 blocks in same and different time range": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2, block3}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 2 * H},
			},
		},
		"recompact one level 1 block with level 2 blocks all in different time range": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 4 * H, MaxTime: 6 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2, block3, block4}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 12 * H},
			},
		},
		"recompact one level 1 block with level 2 blocks and level 3 block": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block5: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 0 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        12 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2, block3}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 2 * H},
			},
		},
		"recompact two level 1 block with level 2 blocks": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2, block3, block4}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 2 * H},
			},
		},
		"recompact one level 1 block with one level 3 block": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        12 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 12 * H},
			},
		},
		"recompact one level 1 block with level 3 blocks": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        12 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        12 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2, block3}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 12 * H},
			},
		},
		"recompact two level 1 block in same time range with level 3 blocks": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        12 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 0 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        12 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 2 * H},
			},
		},
		"recompact two level 1 block in different time range with level 3 blocks": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 10 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        12 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 0 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        12 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2, block3, block4}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 12 * H},
			},
		},
		"recompact one level 1 block with one level 4 block": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 22 * H, MaxTime: 24 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 24 * H, Compaction: tsdb.BlockMetaCompaction{Level: 4}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        24 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 24 * H},
			},
		},
		"recompact one level 1 block with level 4 blocks": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 24 * H, Compaction: tsdb.BlockMetaCompaction{Level: 4}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        24 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0 * H, MaxTime: 24 * H, Compaction: tsdb.BlockMetaCompaction{Level: 4}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        24 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2, block3}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 24 * H},
			},
		},
		"recompact two level 1 blocks in different time range with level 4 blocks": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 22 * H, MaxTime: 24 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 24 * H, Compaction: tsdb.BlockMetaCompaction{Level: 4}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        24 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0 * H, MaxTime: 24 * H, Compaction: tsdb.BlockMetaCompaction{Level: 4}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        24 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2, block3, block4}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 24 * H},
			},
		},
		"recompact one level 2 block with level 3 blocks": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        12 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        12 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2, block3}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 12 * H},
			},
		},
		"recompact two level 2 blocks from different time range with level 3 blocks": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        12 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 0 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        12 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2, block3, block4}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 12 * H},
			},
		},
		"recompact two level 2 blocks from same time range with level 3 blocks": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        12 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 0 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        12 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2, block3, block4}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 12 * H},
			},
		},
		"recompact one level 2 block with level 4 blocks": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 24 * H, Compaction: tsdb.BlockMetaCompaction{Level: 4}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        24 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0 * H, MaxTime: 24 * H, Compaction: tsdb.BlockMetaCompaction{Level: 4}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        24 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2, block3}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 24 * H},
			},
		},
		"recompact two level 2 blocks from different time range with level 4 blocks": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0 * H, MaxTime: 24 * H, Compaction: tsdb.BlockMetaCompaction{Level: 4}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        24 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 0 * H, MaxTime: 24 * H, Compaction: tsdb.BlockMetaCompaction{Level: 4}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        24 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 12 * H},
			},
		},
		"recompact two level 2 blocks from same time range with level 4 blocks": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0 * H, MaxTime: 24 * H, Compaction: tsdb.BlockMetaCompaction{Level: 4}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        24 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 0 * H, MaxTime: 24 * H, Compaction: tsdb.BlockMetaCompaction{Level: 4}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        24 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 12 * H},
			},
		},
		"recompact one level 3 block with level 4 blocks": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        12 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 24 * H, Compaction: tsdb.BlockMetaCompaction{Level: 4}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        24 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0 * H, MaxTime: 24 * H, Compaction: tsdb.BlockMetaCompaction{Level: 4}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        24 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2, block3}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 24 * H},
			},
		},
		"recompact two level 3 blocks from different time range with level 4 blocks": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        12 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 12 * H, MaxTime: 24 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        12 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0 * H, MaxTime: 24 * H, Compaction: tsdb.BlockMetaCompaction{Level: 4}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        24 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 0 * H, MaxTime: 24 * H, Compaction: tsdb.BlockMetaCompaction{Level: 4}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        24 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2, block3, block4}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 24 * H},
			},
		},
		"recompact two level 3 blocks from same time range with level 4 blocks": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        12 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        12 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0 * H, MaxTime: 24 * H, Compaction: tsdb.BlockMetaCompaction{Level: 4}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        24 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 0 * H, MaxTime: 24 * H, Compaction: tsdb.BlockMetaCompaction{Level: 4}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        24 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2, block3, block4}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 24 * H},
			},
		},
		"blocks with partition info should be assigned to correct partition": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}, Stats: tsdb.BlockStats{NumSeries: 1}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 4, PartitionID: 0}}, Files: []metadata.File{{RelPath: thanosblock.IndexFilename, SizeBytes: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}, Stats: tsdb.BlockStats{NumSeries: 1}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 4, PartitionID: 1}}, Files: []metadata.File{{RelPath: thanosblock.IndexFilename, SizeBytes: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}, Stats: tsdb.BlockStats{NumSeries: 1}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 4, PartitionID: 2}}, Files: []metadata.File{{RelPath: thanosblock.IndexFilename, SizeBytes: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block4: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block4, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}, Stats: tsdb.BlockStats{NumSeries: 1}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 4, PartitionID: 3}}, Files: []metadata.File{{RelPath: thanosblock.IndexFilename, SizeBytes: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block5: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block5, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}, Stats: tsdb.BlockStats{NumSeries: 1}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}, Files: []metadata.File{{RelPath: thanosblock.IndexFilename, SizeBytes: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block6: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block6, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}, Stats: tsdb.BlockStats{NumSeries: 1}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 1}}, Files: []metadata.File{{RelPath: thanosblock.IndexFilename, SizeBytes: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block7: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block7, MinTime: 4 * H, MaxTime: 6 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}, Stats: tsdb.BlockStats{NumSeries: 1}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}, Files: []metadata.File{{RelPath: thanosblock.IndexFilename, SizeBytes: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block3, block5, block7}, partitionCount: 2, partitionID: 0, rangeStart: 0 * H, rangeEnd: 12 * H},
				{blocks: []ulid.ULID{block2, block4, block6, block7}, partitionCount: 2, partitionID: 1, rangeStart: 0 * H, rangeEnd: 12 * H},
			},
		},
		"one of the partitions got only one block": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}, Stats: tsdb.BlockStats{NumSeries: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}, Files: []metadata.File{{RelPath: thanosblock.IndexFilename, SizeBytes: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}, Stats: tsdb.BlockStats{NumSeries: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}, Files: []metadata.File{{RelPath: thanosblock.IndexFilename, SizeBytes: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}, Stats: tsdb.BlockStats{NumSeries: 2}},
						Thanos:    metadata.Thanos{Files: []metadata.File{{RelPath: thanosblock.IndexFilename, SizeBytes: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2, block3}, partitionCount: 2, partitionID: 0, rangeStart: 0 * H, rangeEnd: 2 * H},
				{blocks: []ulid.ULID{block3, DUMMY_BLOCK_ID}, partitionCount: 2, partitionID: 1, rangeStart: 0 * H, rangeEnd: 2 * H},
			},
		},
		"not all level 2 blocks are in bucket index": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 2 * H, MaxTime: 4 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			existingPartitionedGroups: []mockExistingPartitionedGroup{
				{rangeStart: 0 * H, rangeEnd: 2 * H, partitionCount: 2, partitions: []Partition{
					{PartitionID: 0, Blocks: []ulid.ULID{ulid.MustNew(99, nil), ulid.MustNew(98, nil)}},
					{PartitionID: 1, Blocks: []ulid.ULID{ulid.MustNew(99, nil), ulid.MustNew(98, nil)}},
				}, partitionVisitMarkers: map[int]mockPartitionVisitMarker{
					0: {partitionID: 0, compactorID: testCompactorID, isExpired: true, status: Completed},
					1: {partitionID: 1, compactorID: testCompactorID, isExpired: true, status: Completed},
				}},
			},
			expected: []expectedCompactionJob{},
		},
		"not all level 2 blocks are in bucket index and there are late level 1 blocks": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block3, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			existingPartitionedGroups: []mockExistingPartitionedGroup{
				{rangeStart: 0 * H, rangeEnd: 2 * H, partitionCount: 2, partitions: []Partition{
					{PartitionID: 0, Blocks: []ulid.ULID{ulid.MustNew(99, nil), ulid.MustNew(98, nil)}},
					{PartitionID: 1, Blocks: []ulid.ULID{ulid.MustNew(99, nil), ulid.MustNew(98, nil)}},
				}, partitionVisitMarkers: map[int]mockPartitionVisitMarker{
					0: {partitionID: 0, compactorID: testCompactorID, isExpired: true, status: Completed},
					1: {partitionID: 1, compactorID: testCompactorID, isExpired: true, status: Completed},
				}},
			},
			expected: []expectedCompactionJob{},
		},
		"level 2 blocks all have same partitioned group id as destination group": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionedGroupID: partitionedGroupID_0_12, PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionedGroupID: partitionedGroupID_0_12, PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			existingPartitionedGroups: []mockExistingPartitionedGroup{},
			expected:                  []expectedCompactionJob{},
		},
		"level 3 blocks all have same partitioned group id as destination group": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionedGroupID: partitionedGroupID_0_24, PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 12 * H, Compaction: tsdb.BlockMetaCompaction{Level: 3}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionedGroupID: partitionedGroupID_0_24, PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			existingPartitionedGroups: []mockExistingPartitionedGroup{},
			expected:                  []expectedCompactionJob{},
		},
		"level 2 blocks not all have same partitioned group id as destination group": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionedGroupID: partitionedGroupID_0_12, PartitionCount: 2, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionedGroupID: partitionedGroupID_0_12, PartitionCount: 2, PartitionID: 1}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block3: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 0 * H, MaxTime: 2 * H, Compaction: tsdb.BlockMetaCompaction{Level: 2}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionedGroupID: partitionedGroupID_0_2, PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			existingPartitionedGroups: []mockExistingPartitionedGroup{},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2, block3}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 12 * H},
			},
		},
		"recompact one level 1 block with level 4 blocks with data only in part of time range across smaller time range": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10*H + 49*M, MaxTime: 11*H + 47*M, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 10*H + 49*M, MaxTime: 16 * H, Compaction: tsdb.BlockMetaCompaction{Level: 4}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        24 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 24 * H},
			},
		},
		"recompact one level 1 block with level 4 blocks with time range in meta and data only in part of time range in same smaller time range": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10*H + 49*M, MaxTime: 11*H + 47*M, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 1 * H, MaxTime: 10 * H, Compaction: tsdb.BlockMetaCompaction{Level: 4}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{TimeRange: 24 * H, PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        24 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 24 * H},
			},
		},
		"recompact one level 1 block with level 4 blocks with no time range in meta and data only in part of time range in same smaller time range": {
			ranges: []time.Duration{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
			blocks: map[ulid.ULID]mockBlock{
				block1: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block1, MinTime: 10*H + 49*M, MaxTime: 11*H + 47*M, Compaction: tsdb.BlockMetaCompaction{Level: 1}},
					},
					timeRange:        2 * time.Hour,
					hasNoCompactMark: false,
				},
				block2: {
					meta: &metadata.Meta{
						BlockMeta: tsdb.BlockMeta{ULID: block2, MinTime: 1 * H, MaxTime: 10 * H, Compaction: tsdb.BlockMetaCompaction{Level: 4}},
						Thanos:    metadata.Thanos{Extensions: cortextsdb.CortexMetaExtensions{PartitionInfo: &cortextsdb.PartitionInfo{PartitionCount: 1, PartitionID: 0}}},
					},
					timeRange:        24 * time.Hour,
					hasNoCompactMark: false,
				},
			},
			expected: []expectedCompactionJob{
				{blocks: []ulid.ULID{block1, block2}, partitionCount: 1, partitionID: 0, rangeStart: 0 * H, rangeEnd: 12 * H},
			},
		},
	}

	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			compactorCfg := &Config{
				BlockRanges: testCase.ranges,
			}

			limits := &validation.Limits{
				CompactorPartitionSeriesCount: 4,
			}
			overrides, err := validation.NewOverrides(*limits, nil)
			require.NoError(t, err)

			// Setup mocking of the ring so that the grouper will own all the shards
			rs := ring.ReplicationSet{
				Instances: []ring.InstanceDesc{
					{Addr: "test-addr"},
				},
			}
			subring := &RingMock{}
			subring.On("GetAllHealthy", mock.Anything).Return(rs, nil)
			subring.On("Get", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(rs, nil)

			ring := &RingMock{}
			ring.On("ShuffleShard", mock.Anything, mock.Anything).Return(subring, nil)

			registerer := prometheus.NewPedanticRegistry()

			metrics := newCompactorMetrics(registerer)

			noCompactFilter := testCase.getNoCompactFilter()

			bkt := &bucket.ClientMock{}
			visitMarkerTimeout := 5 * time.Minute
			testCase.setupBucketStore(t, bkt, userID, visitMarkerTimeout)
			bkt.MockUpload(mock.Anything, nil)
			bkt.MockGet(mock.Anything, "", nil)
			bkt.MockIter(mock.Anything, nil, nil)

			for _, b := range testCase.blocks {
				b.fixPartitionInfo(t, userID)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ingestionReplicationFactor := 1
			if testCase.ingestionReplicationFactor > 1 {
				ingestionReplicationFactor = testCase.ingestionReplicationFactor
			}
			g := NewPartitionCompactionGrouper(
				ctx,
				nil,
				objstore.WithNoopInstr(bkt),
				false, // Do not accept malformed indexes
				true,  // Enable vertical compaction
				nil,
				metrics.getSyncerMetrics(userID),
				metrics,
				metadata.NoneFunc,
				*compactorCfg,
				ring,
				"test-addr",
				testCompactorID,
				overrides,
				userID,
				10,
				3,
				1,
				false,
				visitMarkerTimeout,
				noCompactFilter,
				ingestionReplicationFactor,
			)
			actual, err := g.generateCompactionJobs(testCase.getBlocks())
			require.NoError(t, err)
			require.Len(t, actual, len(testCase.expected))

			for idx, expectedGroup := range testCase.expected {
				actualGroup := actual[idx]
				actualBlocks := actualGroup.blocks
				require.Equal(t, expectedGroup.rangeStart, actualGroup.partitionedGroupInfo.RangeStart)
				require.Equal(t, expectedGroup.rangeEnd, actualGroup.partitionedGroupInfo.RangeEnd)
				require.Equal(t, expectedGroup.partitionCount, actualGroup.partitionedGroupInfo.PartitionCount)
				require.Equal(t, expectedGroup.partitionID, actualGroup.partition.PartitionID)
				require.Len(t, actualBlocks, len(expectedGroup.blocks))
				for _, b := range actualBlocks {
					require.Contains(t, expectedGroup.blocks, b.ULID)
				}
			}
		})
	}
}

type generateCompactionJobsTestCase struct {
	ranges                     []time.Duration
	blocks                     map[ulid.ULID]mockBlock
	existingPartitionedGroups  []mockExistingPartitionedGroup
	expected                   []expectedCompactionJob
	ingestionReplicationFactor int
}

func (g *generateCompactionJobsTestCase) setupBucketStore(t *testing.T, bkt *bucket.ClientMock, userID string, visitMarkerTimeout time.Duration) {
	var existingPartitionedGroupFiles []string
	for _, existingPartitionedGroup := range g.existingPartitionedGroups {
		partitionedGroupFilePath := existingPartitionedGroup.setupBucketStore(t, bkt, userID, visitMarkerTimeout)
		existingPartitionedGroupFiles = append(existingPartitionedGroupFiles, partitionedGroupFilePath)
	}
	bkt.MockIter(PartitionedGroupDirectory, existingPartitionedGroupFiles, nil)
}

func (g *generateCompactionJobsTestCase) getNoCompactFilter() func() map[ulid.ULID]*metadata.NoCompactMark {
	noCompactBlocks := make(map[ulid.ULID]*metadata.NoCompactMark)
	for id, b := range g.blocks {
		if b.hasNoCompactMark {
			noCompactBlocks[id] = &metadata.NoCompactMark{
				ID:            id,
				NoCompactTime: time.Now().Add(-1 * time.Hour).Unix(),
			}
		}
	}
	return func() map[ulid.ULID]*metadata.NoCompactMark {
		return noCompactBlocks
	}
}

func (g *generateCompactionJobsTestCase) getBlocks() map[ulid.ULID]*metadata.Meta {
	blocks := make(map[ulid.ULID]*metadata.Meta)
	for id, b := range g.blocks {
		blocks[id] = b.meta
	}
	return blocks
}

type mockExistingPartitionedGroup struct {
	partitionedGroupID    uint32
	rangeStart            int64
	rangeEnd              int64
	partitionCount        int
	partitions            []Partition
	partitionVisitMarkers map[int]mockPartitionVisitMarker
}

func (p *mockExistingPartitionedGroup) updatePartitionedGroupID(userID string) {
	p.partitionedGroupID = hashGroup(userID, p.rangeStart, p.rangeEnd)
}

func (p *mockExistingPartitionedGroup) setupBucketStore(t *testing.T, bkt *bucket.ClientMock, userID string, visitMarkerTimeout time.Duration) string {
	p.updatePartitionedGroupID(userID)
	partitionedGroupFilePath := path.Join(PartitionedGroupDirectory, fmt.Sprintf("%d.json", p.partitionedGroupID))
	for _, partition := range p.partitions {
		partitionID := partition.PartitionID
		if _, ok := p.partitionVisitMarkers[partitionID]; !ok {
			continue
		}
		visitMarker := p.partitionVisitMarkers[partitionID]
		partitionVisitMarkerFilePath := path.Join(PartitionedGroupDirectory, PartitionVisitMarkerDirectory,
			fmt.Sprintf("%d/%s%d-%s", p.partitionedGroupID, PartitionVisitMarkerFilePrefix, partitionID, PartitionVisitMarkerFileSuffix))
		visitTime := time.Now()
		if visitMarker.isExpired {
			visitTime = time.Now().Add(-2 * visitMarkerTimeout)
		}
		actualVisitMarker := partitionVisitMarker{
			CompactorID:        visitMarker.compactorID,
			Status:             visitMarker.status,
			PartitionedGroupID: p.partitionedGroupID,
			PartitionID:        partitionID,
			VisitTime:          visitTime.UnixMilli(),
			Version:            PartitionVisitMarkerVersion1,
		}
		partitionVisitMarkerContent, err := json.Marshal(actualVisitMarker)
		require.NoError(t, err)
		bkt.MockGet(partitionVisitMarkerFilePath, string(partitionVisitMarkerContent), nil)
	}
	partitionedGroup := PartitionedGroupInfo{
		PartitionedGroupID: p.partitionedGroupID,
		PartitionCount:     p.partitionCount,
		Partitions:         p.partitions,
		RangeStart:         p.rangeStart,
		RangeEnd:           p.rangeEnd,
		CreationTime:       time.Now().Add(-1 * time.Minute).Unix(),
		Version:            PartitionedGroupInfoVersion1,
	}
	partitionedGroupContent, err := json.Marshal(partitionedGroup)
	require.NoError(t, err)
	bkt.MockGet(partitionedGroupFilePath, string(partitionedGroupContent), nil)
	return partitionedGroupFilePath
}

type mockBlock struct {
	meta             *metadata.Meta
	timeRange        time.Duration
	hasNoCompactMark bool
}

func (b *mockBlock) fixPartitionInfo(t *testing.T, userID string) {
	extensions, err := cortextsdb.GetCortexMetaExtensionsFromMeta(*b.meta)
	require.NoError(t, err)
	if extensions != nil {
		rangeStart := getRangeStart(b.meta, b.timeRange.Milliseconds())
		rangeEnd := rangeStart + b.timeRange.Milliseconds()
		if extensions.PartitionInfo.PartitionedGroupID == 0 {
			extensions.PartitionInfo.PartitionedGroupID = hashGroup(userID, rangeStart, rangeEnd)
		}
		b.meta.Thanos.Extensions = extensions
	}
}

type mockPartitionVisitMarker struct {
	partitionID int
	compactorID string
	isExpired   bool
	status      VisitStatus
}

type expectedCompactionJob struct {
	blocks         []ulid.ULID
	partitionCount int
	partitionID    int
	rangeStart     int64
	rangeEnd       int64
}
