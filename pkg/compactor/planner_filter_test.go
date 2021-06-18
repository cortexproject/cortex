package compactor

import (
	"testing"

	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

func TestGroupBlocksByCompactableRanges(t *testing.T) {
	tests := map[string]struct {
		ranges   []int64
		blocks   []*metadata.Meta
		expected []blocksGroup
	}{
		"no input blocks": {
			ranges:   []int64{20},
			blocks:   nil,
			expected: nil,
		},
		"only 1 block in input": {
			ranges: []int64{20},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
			},
			expected: nil,
		},
		"only 1 block for each range (single range)": {
			ranges: []int64{20},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 20, MaxTime: 30}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 40, MaxTime: 60}},
			},
			expected: nil,
		},
		"only 1 block for each range (multiple ranges)": {
			ranges: []int64{10, 20},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 20, MaxTime: 30}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 40, MaxTime: 60}},
			},
			expected: nil,
		},
		"input blocks can be compacted on the 1st range only": {
			ranges: []int64{20, 40},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 20, MaxTime: 30}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 25, MaxTime: 30}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 30, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 40, MaxTime: 50}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 50, MaxTime: 60}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 60, MaxTime: 70}},
			},
			expected: []blocksGroup{
				{rangeStart: 20, rangeEnd: 40, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 20, MaxTime: 30}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 25, MaxTime: 30}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 30, MaxTime: 40}},
				}},
				{rangeStart: 40, rangeEnd: 60, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 40, MaxTime: 50}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 50, MaxTime: 60}},
				}},
			},
		},
		"input blocks can be compacted on the 2nd range only": {
			ranges: []int64{10, 20},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 20, MaxTime: 30}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 30, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 40, MaxTime: 60}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 60, MaxTime: 70}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 70, MaxTime: 80}},
			},
			expected: []blocksGroup{
				{rangeStart: 20, rangeEnd: 40, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 20, MaxTime: 30}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 30, MaxTime: 40}},
				}},
				{rangeStart: 60, rangeEnd: 80, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 60, MaxTime: 70}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 70, MaxTime: 80}},
				}},
			},
		},
		"input blocks can be compacted on a mix of 1st and 2nd ranges, guaranteeing no overlaps and giving preference to smaller ranges": {
			ranges: []int64{10, 20},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 10}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 7, MaxTime: 10}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 20, MaxTime: 30}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 30, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 40, MaxTime: 60}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 60, MaxTime: 70}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 70, MaxTime: 80}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 75, MaxTime: 80}},
			},
			expected: []blocksGroup{
				{rangeStart: 0, rangeEnd: 10, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 10}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 7, MaxTime: 10}},
				}},
				{rangeStart: 70, rangeEnd: 80, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 70, MaxTime: 80}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 75, MaxTime: 80}},
				}},
				{rangeStart: 20, rangeEnd: 40, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 20, MaxTime: 30}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 30, MaxTime: 40}},
				}},
			},
		},
		"input blocks have already been compacted with the largest range": {
			ranges: []int64{10, 20, 40},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 40, MaxTime: 70}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 80, MaxTime: 120}},
			},
			expected: nil,
		},
		"input blocks match the largest range but can be compacted because overlapping": {
			ranges: []int64{10, 20, 40},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 40, MaxTime: 70}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 80, MaxTime: 120}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 80, MaxTime: 120}},
			},
			expected: []blocksGroup{
				{rangeStart: 80, rangeEnd: 120, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 80, MaxTime: 120}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 80, MaxTime: 120}},
				}},
			},
		},
		"a block with time range crossing two 1st level ranges should be NOT considered for 1st level compaction": {
			ranges: []int64{20, 40},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 30}}, // This block spans across two 1st level ranges.
				{BlockMeta: tsdb.BlockMeta{MinTime: 20, MaxTime: 30}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 30, MaxTime: 40}},
			},
			expected: []blocksGroup{
				{rangeStart: 20, rangeEnd: 40, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 20, MaxTime: 30}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 30, MaxTime: 40}},
				}},
			},
		},
		"a block with time range crossing two 1st level ranges should BE considered for 2nd level compaction": {
			ranges: []int64{20, 40},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 30}}, // This block spans across two 1st level ranges.
				{BlockMeta: tsdb.BlockMeta{MinTime: 20, MaxTime: 40}},
			},
			expected: []blocksGroup{
				{rangeStart: 0, rangeEnd: 40, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 20}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 30}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 20, MaxTime: 40}},
				}},
			},
		},
		"a block with time range larger then the largest compaction range should NOT be considered for compaction": {
			ranges: []int64{10, 20, 40},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 30, MaxTime: 150}}, // This block is larger then the largest compaction range.
				{BlockMeta: tsdb.BlockMeta{MinTime: 40, MaxTime: 70}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 80, MaxTime: 120}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 80, MaxTime: 120}},
			},
			expected: []blocksGroup{
				{rangeStart: 80, rangeEnd: 120, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 80, MaxTime: 120}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 80, MaxTime: 120}},
				}},
			},
		},
		"a range containg the most recent block shouldn't be prematurely compacted if doesn't cover the full range": {
			ranges: []int64{10, 20, 40},
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 5, MaxTime: 8}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 7, MaxTime: 9}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 12}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 13, MaxTime: 15}},
			},
			expected: []blocksGroup{
				{rangeStart: 0, rangeEnd: 10, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 5, MaxTime: 8}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 7, MaxTime: 9}},
				}},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, groupBlocksByCompactableRanges(testData.blocks, testData.ranges))
		})
	}
}

func TestGroupBlocksByRange(t *testing.T) {
	tests := map[string]struct {
		timeRange int64
		blocks    []*metadata.Meta
		expected  []blocksGroup
	}{
		"no input blocks": {
			timeRange: 20,
			blocks:    nil,
			expected:  nil,
		},
		"only 1 block in input": {
			timeRange: 20,
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
			},
			expected: []blocksGroup{
				{rangeStart: 0, rangeEnd: 20, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				}},
			},
		},
		"only 1 block per range": {
			timeRange: 20,
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 15}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 40, MaxTime: 60}},
			},
			expected: []blocksGroup{
				{rangeStart: 0, rangeEnd: 20, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 15}},
				}},
				{rangeStart: 40, rangeEnd: 60, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 40, MaxTime: 60}},
				}},
			},
		},
		"multiple blocks per range": {
			timeRange: 20,
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 15}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 40, MaxTime: 60}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 50, MaxTime: 55}},
			},
			expected: []blocksGroup{
				{rangeStart: 0, rangeEnd: 20, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 15}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				}},
				{rangeStart: 40, rangeEnd: 60, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 40, MaxTime: 60}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 50, MaxTime: 55}},
				}},
			},
		},
		"a block with time range larger then the range should be excluded": {
			timeRange: 20,
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 40}}, // This block is larger then the range.
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 20, MaxTime: 30}},
			},
			expected: []blocksGroup{
				{rangeStart: 0, rangeEnd: 20, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 20}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				}},
				{rangeStart: 20, rangeEnd: 40, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 20, MaxTime: 30}},
				}},
			},
		},
		"blocks with different time ranges but all fitting within the input range": {
			timeRange: 40,
			blocks: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 40}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
				{BlockMeta: tsdb.BlockMeta{MinTime: 20, MaxTime: 30}},
			},
			expected: []blocksGroup{
				{rangeStart: 0, rangeEnd: 40, blocks: []*metadata.Meta{
					{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 20}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 40}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 10, MaxTime: 20}},
					{BlockMeta: tsdb.BlockMeta{MinTime: 20, MaxTime: 30}},
				}},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, groupBlocksByRange(testData.blocks, testData.timeRange))
		})
	}
}

func TestBlocksGroup_overlaps(t *testing.T) {
	tests := []struct {
		first    blocksGroup
		second   blocksGroup
		expected bool
	}{
		{
			first:    blocksGroup{rangeStart: 10, rangeEnd: 20},
			second:   blocksGroup{rangeStart: 20, rangeEnd: 30},
			expected: false,
		}, {
			first:    blocksGroup{rangeStart: 10, rangeEnd: 20},
			second:   blocksGroup{rangeStart: 19, rangeEnd: 30},
			expected: true,
		}, {
			first:    blocksGroup{rangeStart: 10, rangeEnd: 21},
			second:   blocksGroup{rangeStart: 20, rangeEnd: 30},
			expected: true,
		}, {
			first:    blocksGroup{rangeStart: 10, rangeEnd: 20},
			second:   blocksGroup{rangeStart: 12, rangeEnd: 18},
			expected: true,
		},
	}

	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.first.overlaps(tc.second))
		assert.Equal(t, tc.expected, tc.second.overlaps(tc.first))
	}
}
