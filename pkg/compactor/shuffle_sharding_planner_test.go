package compactor

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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
	}

	currentCompactor := "test-compactor"
	otherCompactor := "other-compactor"

	block1ulid := ulid.MustNew(1, nil)
	block2ulid := ulid.MustNew(2, nil)
	block3ulid := ulid.MustNew(3, nil)

	tests := map[string]struct {
		ranges          []int64
		noCompactBlocks map[ulid.ULID]*metadata.NoCompactMark
		blocks          []*metadata.Meta
		expected        []*metadata.Meta
		expectedErr     error
		visitedBlocks   []VisitedBlock
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
				},
				{
					id:          block2ulid,
					isExpired:   false,
					compactorID: currentCompactor,
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
			visitedBlocks: []VisitedBlock{
				{
					id:          block1ulid,
					isExpired:   false,
					compactorID: currentCompactor,
				},
				{
					id:          block2ulid,
					isExpired:   false,
					compactorID: currentCompactor,
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
			visitedBlocks: []VisitedBlock{
				{
					id:          block1ulid,
					isExpired:   false,
					compactorID: currentCompactor,
				},
				{
					id:          block2ulid,
					isExpired:   false,
					compactorID: currentCompactor,
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
			visitedBlocks: []VisitedBlock{
				{
					id:          block1ulid,
					isExpired:   false,
					compactorID: currentCompactor,
				},
				{
					id:          block2ulid,
					isExpired:   false,
					compactorID: currentCompactor,
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
			visitedBlocks: []VisitedBlock{
				{
					id:          block1ulid,
					isExpired:   false,
					compactorID: currentCompactor,
				},
				{
					id:          block2ulid,
					isExpired:   false,
					compactorID: currentCompactor,
				},
				{
					id:          block3ulid,
					isExpired:   false,
					compactorID: currentCompactor,
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
			visitedBlocks: []VisitedBlock{
				{
					id:          block1ulid,
					isExpired:   false,
					compactorID: currentCompactor,
				},
				{
					id:          block2ulid,
					isExpired:   false,
					compactorID: currentCompactor,
				},
			},
			expected: []*metadata.Meta{},
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
				},
			},
			expected: []*metadata.Meta{},
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
				},
			},
			expected: []*metadata.Meta{},
		},
	}

	blockVisitMarkerTimeout := 5 * time.Minute
	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			bkt := &bucket.ClientMock{}
			for _, visitedBlock := range testData.visitedBlocks {
				visitMarkerFile := path.Join(visitedBlock.id.String(), BlockVisitMarkerFile)
				expireTime := time.Now()
				if visitedBlock.isExpired {
					expireTime = expireTime.Add(-1 * blockVisitMarkerTimeout)
				}
				blockVisitMarker := BlockVisitMarker{
					CompactorID: visitedBlock.compactorID,
					VisitTime:   expireTime.Unix(),
					Version:     VisitMarkerVersion1,
				}
				visitMarkerFileContent, _ := json.Marshal(blockVisitMarker)
				bkt.MockGet(visitMarkerFile, string(visitMarkerFileContent), nil)
			}
			bkt.MockUpload(mock.Anything, nil)

			registerer := prometheus.NewPedanticRegistry()
			blockVisitMarkerReadFailed := promauto.With(registerer).NewCounter(prometheus.CounterOpts{
				Name: "cortex_compactor_block_visit_marker_read_failed",
				Help: "Number of block visit marker file failed to be read.",
			})
			blockVisitMarkerWriteFailed := promauto.With(registerer).NewCounter(prometheus.CounterOpts{
				Name: "cortex_compactor_block_visit_marker_write_failed",
				Help: "Number of block visit marker file failed to be written.",
			})

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
			actual, err := p.Plan(context.Background(), testData.blocks, nil, nil)

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
