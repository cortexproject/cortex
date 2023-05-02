package compactor

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/util/concurrency"
)

func TestShuffleShardingPlanner_Plan(t *testing.T) {
	type VisitedBlock struct {
		id          ulid.ULID
		isExpired   bool
		compactorID string
		status      VisitStatus
	}

	currentCompactor := "test-compactor"
	otherCompactor := "other-compactor"

	block1ulid := ulid.MustNew(1, nil)
	block2ulid := ulid.MustNew(2, nil)
	block3ulid := ulid.MustNew(3, nil)

	partitionID0 := 0

	tests := map[string]struct {
		ranges           []int64
		noCompactBlocks  map[ulid.ULID]*metadata.NoCompactMark
		blocks           []*metadata.Meta
		expected         []*metadata.Meta
		expectedErr      error
		visitedBlocks    []VisitedBlock
		partitionGroupID uint32
		partitionID      int
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
			visitedBlocks: []VisitedBlock{
				{
					id:          block1ulid,
					isExpired:   false,
					compactorID: currentCompactor,
					status:      Pending,
				},
				{
					id:          block2ulid,
					isExpired:   false,
					compactorID: currentCompactor,
					status:      Pending,
				},
			},
			partitionGroupID: 12345,
			partitionID:      partitionID0,
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
			visitedBlocks: []VisitedBlock{
				{
					id:          block1ulid,
					isExpired:   false,
					compactorID: currentCompactor,
					status:      Pending,
				},
				{
					id:          block2ulid,
					isExpired:   false,
					compactorID: currentCompactor,
					status:      Pending,
				},
			},
			partitionGroupID: 12345,
			partitionID:      partitionID0,
			expectedErr:      fmt.Errorf("block %s with time range %d:%d is outside the largest expected range %d:%d", block2ulid.String(), 0*time.Hour.Milliseconds(), 2*time.Hour.Milliseconds(), 2*time.Hour.Milliseconds(), 4*time.Hour.Milliseconds()),
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
			visitedBlocks: []VisitedBlock{
				{
					id:          block1ulid,
					isExpired:   false,
					compactorID: currentCompactor,
					status:      Pending,
				},
				{
					id:          block2ulid,
					isExpired:   false,
					compactorID: currentCompactor,
					status:      Pending,
				},
			},
			partitionGroupID: 12345,
			partitionID:      partitionID0,
			expectedErr:      fmt.Errorf("block %s with time range %d:%d is outside the largest expected range %d:%d", block1ulid.String(), 0*time.Hour.Milliseconds(), 4*time.Hour.Milliseconds(), 0*time.Hour.Milliseconds(), 2*time.Hour.Milliseconds()),
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
			visitedBlocks: []VisitedBlock{
				{
					id:          block1ulid,
					isExpired:   false,
					compactorID: currentCompactor,
					status:      Pending,
				},
				{
					id:          block2ulid,
					isExpired:   false,
					compactorID: currentCompactor,
					status:      Pending,
				},
			},
			partitionGroupID: 12345,
			partitionID:      partitionID0,
			expectedErr:      fmt.Errorf("block %s with time range %d:%d is outside the largest expected range %d:%d", block2ulid.String(), 0*time.Hour.Milliseconds(), 4*time.Hour.Milliseconds(), 0*time.Hour.Milliseconds(), 2*time.Hour.Milliseconds()),
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
			visitedBlocks: []VisitedBlock{
				{
					id:          block1ulid,
					isExpired:   false,
					compactorID: currentCompactor,
					status:      Pending,
				},
				{
					id:          block2ulid,
					isExpired:   false,
					compactorID: currentCompactor,
					status:      Pending,
				},
				{
					id:          block3ulid,
					isExpired:   false,
					compactorID: currentCompactor,
					status:      Pending,
				},
			},
			partitionGroupID: 12345,
			partitionID:      partitionID0,
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
			visitedBlocks: []VisitedBlock{
				{
					id:          block1ulid,
					isExpired:   false,
					compactorID: currentCompactor,
					status:      Pending,
				},
				{
					id:          block2ulid,
					isExpired:   false,
					compactorID: currentCompactor,
					status:      Pending,
				},
			},
			partitionGroupID: 12345,
			partitionID:      partitionID0,
			expected:         []*metadata.Meta{},
		},
		"test should not compact if visit marker file is not expired and visited by other compactor": {
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
			visitedBlocks: []VisitedBlock{
				{
					id:          block1ulid,
					isExpired:   false,
					compactorID: otherCompactor,
					status:      Pending,
				},
			},
			partitionGroupID: 12345,
			partitionID:      partitionID0,
			expected:         []*metadata.Meta{},
		},
		"test should not compact if visit marker file is expired": {
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
			visitedBlocks: []VisitedBlock{
				{
					id:          block1ulid,
					isExpired:   true,
					compactorID: currentCompactor,
					status:      Pending,
				},
			},
			partitionGroupID: 12345,
			partitionID:      partitionID0,
			expected:         []*metadata.Meta{},
		},
		"test should not compact if visit marker file has completed status": {
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
			visitedBlocks: []VisitedBlock{
				{
					id:          block1ulid,
					isExpired:   false,
					compactorID: currentCompactor,
					status:      Completed,
				},
			},
			partitionGroupID: 12345,
			partitionID:      partitionID0,
			expectedErr:      fmt.Errorf("block %s with partition ID %d is in completed status", block1ulid.String(), partitionID0),
		},
	}

	blockVisitMarkerTimeout := 5 * time.Minute
	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			bkt := &bucket.ClientMock{}
			for _, visitedBlock := range testData.visitedBlocks {
				visitMarkerFile := getBlockVisitMarkerFile(visitedBlock.id.String(), testData.partitionID)
				expireTime := time.Now()
				if visitedBlock.isExpired {
					expireTime = expireTime.Add(-1 * blockVisitMarkerTimeout)
				}
				blockVisitMarker := BlockVisitMarker{
					CompactorID:        visitedBlock.compactorID,
					VisitTime:          expireTime.Unix(),
					Version:            VisitMarkerVersion1,
					Status:             visitedBlock.status,
					PartitionedGroupID: testData.partitionGroupID,
					PartitionID:        testData.partitionID,
				}
				visitMarkerFileContent, _ := json.Marshal(blockVisitMarker)
				bkt.MockGet(visitMarkerFile, string(visitMarkerFileContent), nil)
			}
			bkt.MockUpload(mock.Anything, nil)

			blockVisitMarkerReadFailed := prometheus.NewCounter(prometheus.CounterOpts{})
			blockVisitMarkerWriteFailed := prometheus.NewCounter(prometheus.CounterOpts{})

			logs := &concurrency.SyncBuffer{}
			logger := log.NewLogfmtLogger(logs)
			p := NewShuffleShardingPlanner(
				context.Background(),
				objstore.WithNoopInstr(bkt),
				logger,
				testData.ranges,
				func() map[ulid.ULID]*metadata.NoCompactMark {
					return testData.noCompactBlocks
				},
				currentCompactor,
				blockVisitMarkerTimeout,
				time.Minute,
				blockVisitMarkerReadFailed,
				blockVisitMarkerWriteFailed,
			)
			actual, err := p.PlanWithPartition(context.Background(), testData.blocks, testData.partitionID, make(chan error))

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
