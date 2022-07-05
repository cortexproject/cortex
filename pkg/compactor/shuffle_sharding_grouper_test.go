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
	block_1hto2h_ext1_ulid := ulid.MustNew(1, nil)
	block_3hto4h_ext1_ulid := ulid.MustNew(2, nil)
	block_0hto1h_ext1_ulid := ulid.MustNew(3, nil)
	block_2hto3h_ext1_ulid := ulid.MustNew(4, nil)
	block_1hto2h_ext2_ulid := ulid.MustNew(5, nil)
	block_0hto1h_ext2_ulid := ulid.MustNew(6, nil)
	block0to1_ext3_ulid := ulid.MustNew(7, nil)
	block_4hto6h_ext2_ulid := ulid.MustNew(8, nil)
	block_6hto8h_ext2_ulid := ulid.MustNew(9, nil)
	block_1hto2h_ext_1_ulid := ulid.MustNew(10, nil)
	block_0hto20h_ext1_ulid := ulid.MustNew(11, nil)
	block_21hto40h_ext1_ulid := ulid.MustNew(12, nil)
	block_21hto40h_ext1_ulid_copy := ulid.MustNew(13, nil)
	block_0hto45m_ext1_ulid := ulid.MustNew(14, nil)
	block_0hto1h30m_ext1_ulid := ulid.MustNew(15, nil)
	block_last1h_ext1_ulid := ulid.MustNew(16, nil)
	block_last1h_ext1_ulid_copy := ulid.MustNew(17, nil)

	blocks :=
		map[ulid.ULID]*metadata.Meta{
			block_1hto2h_ext1_ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block_1hto2h_ext1_ulid, MinTime: 1 * time.Hour.Milliseconds(), MaxTime: 2 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block_3hto4h_ext1_ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block_3hto4h_ext1_ulid, MinTime: 3 * time.Hour.Milliseconds(), MaxTime: 4 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block_0hto1h_ext1_ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block_0hto1h_ext1_ulid, MinTime: 0 * time.Hour.Milliseconds(), MaxTime: 1 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block_2hto3h_ext1_ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block_2hto3h_ext1_ulid, MinTime: 2 * time.Hour.Milliseconds(), MaxTime: 3 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block_1hto2h_ext2_ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block_1hto2h_ext2_ulid, MinTime: 1 * time.Hour.Milliseconds(), MaxTime: 2 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "2"}},
			},
			block_0hto1h_ext2_ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block_0hto1h_ext2_ulid, MinTime: 0 * time.Hour.Milliseconds(), MaxTime: 1 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "2"}},
			},
			block0to1_ext3_ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block0to1_ext3_ulid, MinTime: 0 * time.Hour.Milliseconds(), MaxTime: 1 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "3"}},
			},
			block_4hto6h_ext2_ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block_4hto6h_ext2_ulid, MinTime: 4 * time.Hour.Milliseconds(), MaxTime: 6 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "2"}},
			},
			block_6hto8h_ext2_ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block_6hto8h_ext2_ulid, MinTime: 6 * time.Hour.Milliseconds(), MaxTime: 8 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "2"}},
			},
			block_1hto2h_ext_1_ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block_1hto2h_ext_1_ulid, MinTime: 1 * time.Hour.Milliseconds(), MaxTime: 2 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block_0hto20h_ext1_ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block_0hto20h_ext1_ulid, MinTime: 0 * time.Hour.Milliseconds(), MaxTime: 20 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block_21hto40h_ext1_ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block_21hto40h_ext1_ulid, MinTime: 21 * time.Hour.Milliseconds(), MaxTime: 40 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block_21hto40h_ext1_ulid_copy: {
				BlockMeta: tsdb.BlockMeta{ULID: block_21hto40h_ext1_ulid_copy, MinTime: 21 * time.Hour.Milliseconds(), MaxTime: 40 * time.Hour.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block_0hto45m_ext1_ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block_0hto45m_ext1_ulid, MinTime: 0 * time.Hour.Milliseconds(), MaxTime: 45 * time.Minute.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block_0hto1h30m_ext1_ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block_0hto1h30m_ext1_ulid, MinTime: 0 * time.Hour.Milliseconds(), MaxTime: 1*time.Hour.Milliseconds() + 30*time.Minute.Milliseconds()},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block_last1h_ext1_ulid: {
				BlockMeta: tsdb.BlockMeta{ULID: block_last1h_ext1_ulid, MinTime: int64(ulid.Now()) - 1*time.Hour.Milliseconds(), MaxTime: int64(ulid.Now())},
				Thanos:    metadata.Thanos{Labels: map[string]string{"external": "1"}},
			},
			block_last1h_ext1_ulid_copy: {
				BlockMeta: tsdb.BlockMeta{ULID: block_last1h_ext1_ulid_copy, MinTime: int64(ulid.Now()) - 1*time.Hour.Milliseconds(), MaxTime: int64(ulid.Now())},
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
			blocks: map[ulid.ULID]*metadata.Meta{block_1hto2h_ext1_ulid: blocks[block_1hto2h_ext1_ulid], block_3hto4h_ext1_ulid: blocks[block_3hto4h_ext1_ulid], block_0hto1h_ext1_ulid: blocks[block_0hto1h_ext1_ulid], block_2hto3h_ext1_ulid: blocks[block_2hto3h_ext1_ulid], block_1hto2h_ext2_ulid: blocks[block_1hto2h_ext2_ulid], block_0hto1h_ext2_ulid: blocks[block_0hto1h_ext2_ulid]},
			expected: [][]ulid.ULID{
				{block_1hto2h_ext2_ulid, block_0hto1h_ext2_ulid},
				{block_1hto2h_ext1_ulid, block_0hto1h_ext1_ulid},
				{block_3hto4h_ext1_ulid, block_2hto3h_ext1_ulid},
			},
		},
		"test no compaction": {
			ranges:   []time.Duration{2 * time.Hour, 4 * time.Hour},
			blocks:   map[ulid.ULID]*metadata.Meta{block_0hto1h_ext1_ulid: blocks[block_0hto1h_ext1_ulid], block_0hto1h_ext2_ulid: blocks[block_0hto1h_ext2_ulid], block0to1_ext3_ulid: blocks[block0to1_ext3_ulid]},
			expected: [][]ulid.ULID{},
		},
		"test smallest range first": {
			ranges: []time.Duration{2 * time.Hour, 4 * time.Hour},
			blocks: map[ulid.ULID]*metadata.Meta{block_1hto2h_ext1_ulid: blocks[block_1hto2h_ext1_ulid], block_3hto4h_ext1_ulid: blocks[block_3hto4h_ext1_ulid], block_0hto1h_ext1_ulid: blocks[block_0hto1h_ext1_ulid], block_2hto3h_ext1_ulid: blocks[block_2hto3h_ext1_ulid], block_4hto6h_ext2_ulid: blocks[block_4hto6h_ext2_ulid], block_6hto8h_ext2_ulid: blocks[block_6hto8h_ext2_ulid]},
			expected: [][]ulid.ULID{
				{block_1hto2h_ext1_ulid, block_0hto1h_ext1_ulid},
				{block_3hto4h_ext1_ulid, block_2hto3h_ext1_ulid},
				{block_4hto6h_ext2_ulid, block_6hto8h_ext2_ulid},
			},
		},
		"test oldest min time first": {
			ranges: []time.Duration{2 * time.Hour, 4 * time.Hour},
			blocks: map[ulid.ULID]*metadata.Meta{block_1hto2h_ext1_ulid: blocks[block_1hto2h_ext1_ulid], block_3hto4h_ext1_ulid: blocks[block_3hto4h_ext1_ulid], block_0hto1h_ext1_ulid: blocks[block_0hto1h_ext1_ulid], block_2hto3h_ext1_ulid: blocks[block_2hto3h_ext1_ulid], block_1hto2h_ext_1_ulid: blocks[block_1hto2h_ext_1_ulid]},
			expected: [][]ulid.ULID{
				{block_1hto2h_ext1_ulid, block_0hto1h_ext1_ulid, block_1hto2h_ext_1_ulid},
				{block_3hto4h_ext1_ulid, block_2hto3h_ext1_ulid},
			},
		},
		"test overlapping blocks": {
			ranges: []time.Duration{20 * time.Hour, 40 * time.Hour},
			blocks: map[ulid.ULID]*metadata.Meta{block_0hto20h_ext1_ulid: blocks[block_0hto20h_ext1_ulid], block_21hto40h_ext1_ulid: blocks[block_21hto40h_ext1_ulid], block_21hto40h_ext1_ulid_copy: blocks[block_21hto40h_ext1_ulid_copy]},
			expected: [][]ulid.ULID{
				{block_21hto40h_ext1_ulid, block_21hto40h_ext1_ulid_copy},
			},
		},
		"test imperfect maxTime blocks": {
			ranges: []time.Duration{2 * time.Hour},
			blocks: map[ulid.ULID]*metadata.Meta{block_0hto1h30m_ext1_ulid: blocks[block_0hto1h30m_ext1_ulid], block_0hto45m_ext1_ulid: blocks[block_0hto45m_ext1_ulid]},
			expected: [][]ulid.ULID{
				{block_0hto45m_ext1_ulid, block_0hto1h30m_ext1_ulid},
			},
		},
		"test prematurely created blocks": {
			ranges:   []time.Duration{2 * time.Hour},
			blocks:   map[ulid.ULID]*metadata.Meta{block_last1h_ext1_ulid_copy: blocks[block_last1h_ext1_ulid_copy], block_last1h_ext1_ulid: blocks[block_last1h_ext1_ulid]},
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
