package compactor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

func TestShuffleShardingPlanner_Plan(t *testing.T) {
	block1ulid := ulid.MustNew(1, nil)
	block2ulid := ulid.MustNew(2, nil)
	block3ulid := ulid.MustNew(3, nil)

	tests := map[string]struct {
		ranges          []int64
		noCompactBlocks map[ulid.ULID]*metadata.NoCompactMark
		blocks          []*metadata.Meta
		expected        []*metadata.Meta
		expectedErr     error
	}{
		"test basic plan": {
			ranges: []int64{2 * time.Hour.Milliseconds()},
			blocks: []*metadata.Meta{
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    block1ulid,
						MinTime: 1 * time.Hour.Milliseconds(),
						MaxTime: 2 * time.Hour.Milliseconds(),
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    block2ulid,
						MinTime: 1 * time.Hour.Milliseconds(),
						MaxTime: 2 * time.Hour.Milliseconds(),
					},
				},
			},
			expected: []*metadata.Meta{
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    block1ulid,
						MinTime: 1 * time.Hour.Milliseconds(),
						MaxTime: 2 * time.Hour.Milliseconds(),
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    block2ulid,
						MinTime: 1 * time.Hour.Milliseconds(),
						MaxTime: 2 * time.Hour.Milliseconds(),
					},
				},
			},
		},
		"test blocks outside largest range smaller min time after": {
			ranges: []int64{2 * time.Hour.Milliseconds()},
			blocks: []*metadata.Meta{
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    block1ulid,
						MinTime: 2 * time.Hour.Milliseconds(),
						MaxTime: 4 * time.Hour.Milliseconds(),
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    block2ulid,
						MinTime: 0 * time.Hour.Milliseconds(),
						MaxTime: 2 * time.Hour.Milliseconds(),
					},
				},
			},
			expectedErr: fmt.Errorf("block %s with time range %d:%d is outside the largest expected range %d:%d", block2ulid.String(), 0*time.Hour.Milliseconds(), 2*time.Hour.Milliseconds(), 2*time.Hour.Milliseconds(), 4*time.Hour.Milliseconds()),
		},
		"test blocks outside largest range 1": {
			ranges: []int64{2 * time.Hour.Milliseconds()},
			blocks: []*metadata.Meta{
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    block1ulid,
						MinTime: 0 * time.Hour.Milliseconds(),
						MaxTime: 4 * time.Hour.Milliseconds(),
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    block2ulid,
						MinTime: 0 * time.Hour.Milliseconds(),
						MaxTime: 4 * time.Hour.Milliseconds(),
					},
				},
			},
			expectedErr: fmt.Errorf("block %s with time range %d:%d is outside the largest expected range %d:%d", block1ulid.String(), 0*time.Hour.Milliseconds(), 4*time.Hour.Milliseconds(), 0*time.Hour.Milliseconds(), 2*time.Hour.Milliseconds()),
		},
		"test blocks outside largest range 2": {
			ranges: []int64{2 * time.Hour.Milliseconds()},
			blocks: []*metadata.Meta{
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    block1ulid,
						MinTime: 0 * time.Hour.Milliseconds(),
						MaxTime: 2 * time.Hour.Milliseconds(),
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    block2ulid,
						MinTime: 0 * time.Hour.Milliseconds(),
						MaxTime: 4 * time.Hour.Milliseconds(),
					},
				},
			},
			expectedErr: fmt.Errorf("block %s with time range %d:%d is outside the largest expected range %d:%d", block2ulid.String(), 0*time.Hour.Milliseconds(), 4*time.Hour.Milliseconds(), 0*time.Hour.Milliseconds(), 2*time.Hour.Milliseconds()),
		},
		"test should skip blocks marked for no compact": {
			ranges:          []int64{2 * time.Hour.Milliseconds()},
			noCompactBlocks: map[ulid.ULID]*metadata.NoCompactMark{block1ulid: {}},
			blocks: []*metadata.Meta{
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    block1ulid,
						MinTime: 1 * time.Hour.Milliseconds(),
						MaxTime: 2 * time.Hour.Milliseconds(),
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    block2ulid,
						MinTime: 1 * time.Hour.Milliseconds(),
						MaxTime: 2 * time.Hour.Milliseconds(),
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    block3ulid,
						MinTime: 1 * time.Hour.Milliseconds(),
						MaxTime: 2 * time.Hour.Milliseconds(),
					},
				},
			},
			expected: []*metadata.Meta{
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    block2ulid,
						MinTime: 1 * time.Hour.Milliseconds(),
						MaxTime: 2 * time.Hour.Milliseconds(),
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    block3ulid,
						MinTime: 1 * time.Hour.Milliseconds(),
						MaxTime: 2 * time.Hour.Milliseconds(),
					},
				},
			},
		},
		"test should not compact if there is only 1 compactable block": {
			ranges:          []int64{2 * time.Hour.Milliseconds()},
			noCompactBlocks: map[ulid.ULID]*metadata.NoCompactMark{block1ulid: {}},
			blocks: []*metadata.Meta{
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    block1ulid,
						MinTime: 1 * time.Hour.Milliseconds(),
						MaxTime: 2 * time.Hour.Milliseconds(),
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    block2ulid,
						MinTime: 1 * time.Hour.Milliseconds(),
						MaxTime: 2 * time.Hour.Milliseconds(),
					},
				},
			},
			expected: []*metadata.Meta{},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			p := NewShuffleShardingPlanner(nil,
				testData.ranges,
				func() map[ulid.ULID]*metadata.NoCompactMark {
					return testData.noCompactBlocks
				},
			)
			actual, err := p.Plan(context.Background(), testData.blocks)

			if testData.expectedErr != nil {
				assert.Equal(t, err, testData.expectedErr)
			} else {
				require.NoError(t, err)
			}

			require.Len(t, actual, len(testData.expected))

			for idx, expectedMeta := range testData.expected {
				assert.Equal(t, expectedMeta.ULID, actual[idx].ULID)
			}
		})
	}
}
