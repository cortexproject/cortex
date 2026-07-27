package compactor

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortextsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util/concurrency"
)

func TestPartitionCompactionPlanner_Plan(t *testing.T) {
	type VisitedPartition struct {
		isExpired   bool
		compactorID string
	}

	currentCompactor := "test-compactor"
	otherCompactor := "other-compactor"

	block1ulid := ulid.MustNew(1, nil)
	block2ulid := ulid.MustNew(2, nil)
	block3ulid := ulid.MustNew(3, nil)

	tests := map[string]struct {
		ranges           []int64
		noCompactBlocks  map[ulid.ULID]*metadata.NoCompactMark
		blocks           []*metadata.Meta
		expected         []*metadata.Meta
		expectedErr      error
		visitedPartition VisitedPartition
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
			visitedPartition: VisitedPartition{
				isExpired:   false,
				compactorID: currentCompactor,
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
			visitedPartition: VisitedPartition{
				isExpired:   false,
				compactorID: currentCompactor,
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
			visitedPartition: VisitedPartition{
				isExpired:   false,
				compactorID: currentCompactor,
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
			visitedPartition: VisitedPartition{
				isExpired:   false,
				compactorID: currentCompactor,
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
			visitedPartition: VisitedPartition{
				isExpired:   false,
				compactorID: currentCompactor,
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
		"test should not compact if there is no compactable block": {
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
			},
			visitedPartition: VisitedPartition{
				isExpired:   false,
				compactorID: currentCompactor,
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
			visitedPartition: VisitedPartition{
				isExpired:   false,
				compactorID: otherCompactor,
			},
			expectedErr: plannerVisitedPartitionError,
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
			visitedPartition: VisitedPartition{
				isExpired:   true,
				compactorID: currentCompactor,
			},
			expectedErr: plannerVisitedPartitionError,
		},
	}

	visitMarkerTimeout := 5 * time.Minute
	partitionedGroupID := uint32(1)
	partitionID := 0
	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			bkt := &bucket.ClientMock{}
			visitMarkerFile := GetPartitionVisitMarkerFilePath(partitionedGroupID, partitionID)
			expireTime := time.Now()
			if testData.visitedPartition.isExpired {
				expireTime = expireTime.Add(-1 * visitMarkerTimeout)
			}
			visitMarker := partitionVisitMarker{
				CompactorID:        testData.visitedPartition.compactorID,
				PartitionedGroupID: partitionedGroupID,
				PartitionID:        partitionID,
				VisitTime:          expireTime.Unix(),
				Status:             Pending,
				Version:            PartitionVisitMarkerVersion1,
			}
			visitMarkerFileContent, _ := json.Marshal(visitMarker)
			// Mock partition group info for race condition fix
			partitionedGroupInfo := PartitionedGroupInfo{
				PartitionedGroupID: partitionedGroupID,
				PartitionCount:     1,
				Partitions: []Partition{
					{PartitionID: partitionID, Blocks: []ulid.ULID{}},
				},
				RangeStart:   0,
				RangeEnd:     2 * time.Hour.Milliseconds(),
				CreationTime: time.Now().Unix(),
				Version:      PartitionedGroupInfoVersion1,
			}
			partitionedGroupContent, _ := json.Marshal(partitionedGroupInfo)
			partitionedGroupFile := GetPartitionedGroupFile(partitionedGroupID)

			bkt.MockGet(visitMarkerFile, string(visitMarkerFileContent), nil)
			bkt.MockGet(partitionedGroupFile, string(partitionedGroupContent), nil)
			bkt.MockUpload(mock.Anything, nil)

			registerer := prometheus.NewPedanticRegistry()

			metrics := newCompactorMetrics(registerer)

			logs := &concurrency.SyncBuffer{}
			logger := log.NewLogfmtLogger(logs)
			p := NewPartitionCompactionPlanner(
				context.Background(),
				objstore.WithNoopInstr(bkt),
				logger,
				testData.ranges,
				func() map[ulid.ULID]*metadata.NoCompactMark {
					return testData.noCompactBlocks
				},
				currentCompactor,
				"test-user",
				10*time.Millisecond,
				visitMarkerTimeout,
				time.Minute,
				metrics,
				nil,
			)
			actual, err := p.Plan(context.Background(), testData.blocks, nil, &cortextsdb.CortexMetaExtensions{
				PartitionInfo: &cortextsdb.PartitionInfo{
					PartitionCount:               1,
					PartitionID:                  partitionID,
					PartitionedGroupID:           partitionedGroupID,
					PartitionedGroupCreationTime: partitionedGroupInfo.CreationTime,
				},
			})

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

func TestPartitionCompactionPlanner_PlanWithDeletionMarkFilter(t *testing.T) {
	currentCompactor := "test-compactor"
	block1ulid := ulid.MustNew(1, nil)
	block2ulid := ulid.MustNew(2, nil)
	block3ulid := ulid.MustNew(3, nil)

	partitionedGroupID := uint32(1)
	partitionID := 0
	visitMarkerTimeout := 5 * time.Minute

	setupBucket := func(t *testing.T, deletionMarkBlockIDs []ulid.ULID) *bucket.ClientMock {
		bkt := &bucket.ClientMock{}

		expireTime := time.Now()
		visitMarker := partitionVisitMarker{
			CompactorID:        currentCompactor,
			PartitionedGroupID: partitionedGroupID,
			PartitionID:        partitionID,
			VisitTime:          expireTime.Unix(),
			Status:             Pending,
			Version:            PartitionVisitMarkerVersion1,
		}
		visitMarkerFileContent, _ := json.Marshal(visitMarker)
		visitMarkerFile := GetPartitionVisitMarkerFilePath(partitionedGroupID, partitionID)

		partitionedGroupInfo := PartitionedGroupInfo{
			PartitionedGroupID: partitionedGroupID,
			PartitionCount:     1,
			Partitions: []Partition{
				{PartitionID: partitionID, Blocks: []ulid.ULID{}},
			},
			RangeStart:   0,
			RangeEnd:     2 * time.Hour.Milliseconds(),
			CreationTime: time.Now().Unix(),
			Version:      PartitionedGroupInfoVersion1,
		}
		partitionedGroupContent, _ := json.Marshal(partitionedGroupInfo)
		partitionedGroupFile := GetPartitionedGroupFile(partitionedGroupID)

		bkt.MockGet(visitMarkerFile, string(visitMarkerFileContent), nil)
		bkt.MockGet(partitionedGroupFile, string(partitionedGroupContent), nil)

		bkt.On("Upload", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// Mock deletion marks for specified blocks
		deletionMarkIDs := make(map[ulid.ULID]struct{})
		for _, id := range deletionMarkBlockIDs {
			deletionMarkIDs[id] = struct{}{}
			deletionMark := metadata.DeletionMark{
				ID:           id,
				Version:      metadata.DeletionMarkVersion1,
				DeletionTime: time.Now().Unix(),
			}
			content, _ := json.Marshal(deletionMark)
			markPath := path.Join(id.String(), metadata.DeletionMarkFilename)
			bkt.MockGet(markPath, string(content), nil)
		}

		// For blocks without deletion marks, mock not-found
		for _, id := range []ulid.ULID{block1ulid, block2ulid, block3ulid} {
			if _, ok := deletionMarkIDs[id]; !ok {
				markPath := path.Join(id.String(), metadata.DeletionMarkFilename)
				bkt.MockGet(markPath, "", nil)
			}
		}

		return bkt
	}

	createPlanner := func(bkt *bucket.ClientMock) *PartitionCompactionPlanner {
		registerer := prometheus.NewPedanticRegistry()
		metrics := newCompactorMetrics(registerer)
		logger := log.NewLogfmtLogger(&concurrency.SyncBuffer{})
		instrBkt := objstore.WithNoopInstr(bkt)
		ignoreDeletionMarkFilter := block.NewIgnoreDeletionMarkFilter(logger, instrBkt, 0, 1)

		return NewPartitionCompactionPlanner(
			context.Background(),
			instrBkt,
			logger,
			[]int64{2 * time.Hour.Milliseconds()},
			func() map[ulid.ULID]*metadata.NoCompactMark { return nil },
			currentCompactor,
			"test-user",
			10*time.Millisecond,
			visitMarkerTimeout,
			time.Minute,
			metrics,
			ignoreDeletionMarkFilter,
		)
	}

	t.Run("should plan successfully when no blocks are marked for deletion", func(t *testing.T) {
		bkt := setupBucket(t, nil)
		partitionedGroupContent := PartitionedGroupInfo{}
		partitionedGroupFile := GetPartitionedGroupFile(partitionedGroupID)
		raw, _ := bkt.Get(context.Background(), partitionedGroupFile)
		buf := make([]byte, 4096)
		n, _ := raw.Read(buf)
		_ = json.Unmarshal(buf[:n], &partitionedGroupContent)

		p := createPlanner(bkt)

		blocks := []*metadata.Meta{
			{BlockMeta: tsdb.BlockMeta{ULID: block1ulid, MinTime: 1 * time.Hour.Milliseconds(), MaxTime: 2 * time.Hour.Milliseconds()}},
			{BlockMeta: tsdb.BlockMeta{ULID: block2ulid, MinTime: 1 * time.Hour.Milliseconds(), MaxTime: 2 * time.Hour.Milliseconds()}},
		}

		actual, err := p.Plan(context.Background(), blocks, nil, &cortextsdb.CortexMetaExtensions{
			PartitionInfo: &cortextsdb.PartitionInfo{
				PartitionCount:               1,
				PartitionID:                  partitionID,
				PartitionedGroupID:           partitionedGroupID,
				PartitionedGroupCreationTime: partitionedGroupContent.CreationTime,
			},
		})

		require.NoError(t, err)
		require.Len(t, actual, 2)
		assert.Equal(t, block1ulid, actual[0].ULID)
		assert.Equal(t, block2ulid, actual[1].ULID)
	})

	t.Run("should fail when blocks are marked for deletion", func(t *testing.T) {
		bkt := setupBucket(t, []ulid.ULID{block2ulid})
		partitionedGroupContent := PartitionedGroupInfo{}
		partitionedGroupFile := GetPartitionedGroupFile(partitionedGroupID)
		raw, _ := bkt.Get(context.Background(), partitionedGroupFile)
		buf := make([]byte, 4096)
		n, _ := raw.Read(buf)
		_ = json.Unmarshal(buf[:n], &partitionedGroupContent)

		p := createPlanner(bkt)

		blocks := []*metadata.Meta{
			{BlockMeta: tsdb.BlockMeta{ULID: block1ulid, MinTime: 1 * time.Hour.Milliseconds(), MaxTime: 2 * time.Hour.Milliseconds()}},
			{BlockMeta: tsdb.BlockMeta{ULID: block2ulid, MinTime: 1 * time.Hour.Milliseconds(), MaxTime: 2 * time.Hour.Milliseconds()}},
			{BlockMeta: tsdb.BlockMeta{ULID: block3ulid, MinTime: 1 * time.Hour.Milliseconds(), MaxTime: 2 * time.Hour.Milliseconds()}},
		}

		actual, err := p.Plan(context.Background(), blocks, nil, &cortextsdb.CortexMetaExtensions{
			PartitionInfo: &cortextsdb.PartitionInfo{
				PartitionCount:               1,
				PartitionID:                  partitionID,
				PartitionedGroupID:           partitionedGroupID,
				PartitionedGroupCreationTime: partitionedGroupContent.CreationTime,
			},
		})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "partitioned group contains 1 deleted blocks")
		assert.Nil(t, actual)
	})

	t.Run("should fail when multiple blocks are marked for deletion", func(t *testing.T) {
		bkt := setupBucket(t, []ulid.ULID{block1ulid, block3ulid})
		partitionedGroupContent := PartitionedGroupInfo{}
		partitionedGroupFile := GetPartitionedGroupFile(partitionedGroupID)
		raw, _ := bkt.Get(context.Background(), partitionedGroupFile)
		buf := make([]byte, 4096)
		n, _ := raw.Read(buf)
		_ = json.Unmarshal(buf[:n], &partitionedGroupContent)

		p := createPlanner(bkt)

		blocks := []*metadata.Meta{
			{BlockMeta: tsdb.BlockMeta{ULID: block1ulid, MinTime: 1 * time.Hour.Milliseconds(), MaxTime: 2 * time.Hour.Milliseconds()}},
			{BlockMeta: tsdb.BlockMeta{ULID: block2ulid, MinTime: 1 * time.Hour.Milliseconds(), MaxTime: 2 * time.Hour.Milliseconds()}},
			{BlockMeta: tsdb.BlockMeta{ULID: block3ulid, MinTime: 1 * time.Hour.Milliseconds(), MaxTime: 2 * time.Hour.Milliseconds()}},
		}

		actual, err := p.Plan(context.Background(), blocks, nil, &cortextsdb.CortexMetaExtensions{
			PartitionInfo: &cortextsdb.PartitionInfo{
				PartitionCount:               1,
				PartitionID:                  partitionID,
				PartitionedGroupID:           partitionedGroupID,
				PartitionedGroupCreationTime: partitionedGroupContent.CreationTime,
			},
		})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "partitioned group contains 2 deleted blocks")
		assert.Nil(t, actual)
	})
}
