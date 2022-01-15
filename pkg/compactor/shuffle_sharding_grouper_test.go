package compactor

import (
	"testing"
	"time"

	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func TestShuffleShardingGrouper_Groups(t *testing.T) {
	block1ulid := ulid.MustNew(1, nil)
	block2ulid := ulid.MustNew(2, nil)
	block3ulid := ulid.MustNew(3, nil)
	block4ulid := ulid.MustNew(4, nil)
	block5ulid := ulid.MustNew(5, nil)
	block6ulid := ulid.MustNew(6, nil)
	block7ulid := ulid.MustNew(7, nil)
	block8ulid := ulid.MustNew(8, nil)
	block9ulid := ulid.MustNew(9, nil)
	block10ulid := ulid.MustNew(10, nil)
	block11ulid := ulid.MustNew(11, nil)
	block12ulid := ulid.MustNew(12, nil)
	block13ulid := ulid.MustNew(13, nil)
	block14ulid := ulid.MustNew(14, nil)
	block15ulid := ulid.MustNew(15, nil)

	blocks :=
		map[ulid.ULID]*metadata.Meta{
			block1ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block1ulid, MinTime: 1 * time.Hour.Milliseconds(), MaxTime: 2 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block2ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block2ulid, MinTime: 3 * time.Hour.Milliseconds(), MaxTime: 4 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block3ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block3ulid, MinTime: 0 * time.Hour.Milliseconds(), MaxTime: 1 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block4ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block4ulid, MinTime: 2 * time.Hour.Milliseconds(), MaxTime: 3 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block5ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block5ulid, MinTime: 1 * time.Hour.Milliseconds(), MaxTime: 2 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "2"}},
			},
			block6ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block6ulid, MinTime: 0 * time.Hour.Milliseconds(), MaxTime: 1 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "2"}},
			},
			block7ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block7ulid, MinTime: 0 * time.Hour.Milliseconds(), MaxTime: 1 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block8ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block8ulid, MinTime: 0 * time.Hour.Milliseconds(), MaxTime: 1 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "2"}},
			},
			block9ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block9ulid, MinTime: 0 * time.Hour.Milliseconds(), MaxTime: 1 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "3"}},
			},
			block10ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block10ulid, MinTime: 4 * time.Hour.Milliseconds(), MaxTime: 6 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "2"}},
			},
			block11ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block11ulid, MinTime: 6 * time.Hour.Milliseconds(), MaxTime: 8 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "2"}},
			},
			block12ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block12ulid, MinTime: 1 * time.Hour.Milliseconds(), MaxTime: 2 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block13ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block13ulid, MinTime: 0 * time.Hour.Milliseconds(), MaxTime: 20 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block14ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block14ulid, MinTime: 21 * time.Hour.Milliseconds(), MaxTime: 40 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block15ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block15ulid, MinTime: 21 * time.Hour.Milliseconds(), MaxTime: 40 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
		}

	tests := map[string]struct {
		ranges   []time.Duration
		blocks   map[ulid.ULID]*metadata.Meta
		expected [][]ulid.ULID
	}{
		"test basic grouping": {
			ranges: []time.Duration{2 * time.Hour, 4 * time.Hour},
			blocks: map[ulid.ULID]*metadata.Meta{block1ulid: blocks[block1ulid], block2ulid: blocks[block2ulid], block3ulid: blocks[block3ulid], block4ulid: blocks[block4ulid], block5ulid: blocks[block5ulid], block6ulid: blocks[block6ulid]},
			expected: [][]ulid.ULID{
				{block5ulid, block6ulid},
				{block1ulid, block3ulid},
				{block2ulid, block4ulid},
			},
		},
		"test no compaction": {
			ranges:   []time.Duration{2 * time.Hour, 4 * time.Hour},
			blocks:   map[ulid.ULID]*metadata.Meta{block7ulid: blocks[block7ulid], block8ulid: blocks[block8ulid], block9ulid: blocks[block9ulid]},
			expected: [][]ulid.ULID{},
		},
		"test smallest range first": {
			ranges: []time.Duration{2 * time.Hour, 4 * time.Hour},
			blocks: map[ulid.ULID]*metadata.Meta{block1ulid: blocks[block1ulid], block2ulid: blocks[block2ulid], block3ulid: blocks[block3ulid], block4ulid: blocks[block4ulid], block10ulid: blocks[block10ulid], block11ulid: blocks[block11ulid]},
			expected: [][]ulid.ULID{
				{block1ulid, block3ulid},
				{block2ulid, block4ulid},
				{block10ulid, block11ulid},
			},
		},
		"test oldest min time first": {
			ranges: []time.Duration{2 * time.Hour, 4 * time.Hour},
			blocks: map[ulid.ULID]*metadata.Meta{block1ulid: blocks[block1ulid], block2ulid: blocks[block2ulid], block3ulid: blocks[block3ulid], block4ulid: blocks[block4ulid], block12ulid: blocks[block12ulid]},
			expected: [][]ulid.ULID{
				{block1ulid, block3ulid, block12ulid},
				{block2ulid, block4ulid},
			},
		},
		"test overlapping blocks": {
			ranges:   []time.Duration{20 * time.Hour, 40 * time.Hour},
			blocks:   map[ulid.ULID]*metadata.Meta{block13ulid: blocks[block13ulid], block14ulid: blocks[block14ulid], block15ulid: blocks[block15ulid]},
			expected: [][]ulid.ULID{},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			compactorCfg := &Config{
				BlockRanges: testData.ranges,
			}

			limits := &validation.Limits{}
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

			registerer := prometheus.NewRegistry()
			remainingPlannedCompactions := promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
				Name: "cortex_compactor_remaining_planned_compactions",
				Help: "Total number of plans that remain to be compacted.",
			})

			g := NewShuffleShardingGrouper(nil,
				nil,
				false, // Do not accept malformed indexes
				true,  // Enable vertical compaction
				registerer,
				nil,
				nil,
				nil,
				remainingPlannedCompactions,
				metadata.NoneFunc,
				*compactorCfg,
				ring,
				"test-addr",
				overrides,
				"")
			actual, err := g.Groups(testData.blocks)
			require.NoError(t, err)
			require.Len(t, actual, len(testData.expected))

			for idx, expectedIDs := range testData.expected {
				assert.Equal(t, expectedIDs, actual[idx].IDs())
			}
		})
	}
}

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
		"a range containing the most recent block shouldn't be prematurely compacted if doesn't cover the full range": {
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

type RingMock struct {
	mock.Mock
}

func (r *RingMock) Collect(ch chan<- prometheus.Metric) {}

func (r *RingMock) Describe(ch chan<- *prometheus.Desc) {}

func (r *RingMock) Get(key uint32, op ring.Operation, bufDescs []ring.InstanceDesc, bufHosts, bufZones []string) (ring.ReplicationSet, error) {
	args := r.Called(key, op, bufDescs, bufHosts, bufZones)
	return args.Get(0).(ring.ReplicationSet), args.Error(1)
}

func (r *RingMock) GetAllHealthy(op ring.Operation) (ring.ReplicationSet, error) {
	args := r.Called(op)
	return args.Get(0).(ring.ReplicationSet), args.Error(1)
}

func (r *RingMock) GetReplicationSetForOperation(op ring.Operation) (ring.ReplicationSet, error) {
	args := r.Called(op)
	return args.Get(0).(ring.ReplicationSet), args.Error(1)
}

func (r *RingMock) ReplicationFactor() int {
	return 0
}

func (r *RingMock) InstancesCount() int {
	return 0
}

func (r *RingMock) ShuffleShard(identifier string, size int) ring.ReadRing {
	args := r.Called(identifier, size)
	return args.Get(0).(ring.ReadRing)
}

func (r *RingMock) GetInstanceState(instanceID string) (ring.InstanceState, error) {
	args := r.Called(instanceID)
	return args.Get(0).(ring.InstanceState), args.Error(1)
}

func (r *RingMock) ShuffleShardWithLookback(identifier string, size int, lookbackPeriod time.Duration, now time.Time) ring.ReadRing {
	args := r.Called(identifier, size, lookbackPeriod, now)
	return args.Get(0).(ring.ReadRing)
}

func (r *RingMock) HasInstance(instanceID string) bool {
	return true
}

func (r *RingMock) CleanupShuffleShardCache(identifier string) {}
