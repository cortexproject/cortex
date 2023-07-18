package compactor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	cortex_testutil "github.com/cortexproject/cortex/pkg/storage/tsdb/testutil"
)

func TestMarkBlocksVisited(t *testing.T) {
	ulid0 := ulid.MustNew(0, nil)
	ulid1 := ulid.MustNew(1, nil)
	ulid2 := ulid.MustNew(2, nil)
	now := time.Now().Unix()
	nowBefore1h := time.Now().Add(-1 * time.Hour).Unix()
	for _, tcase := range []struct {
		name        string
		visitMarker BlockVisitMarker
		blocks      []*metadata.Meta
	}{
		{
			name: "write visit marker succeeded",
			visitMarker: BlockVisitMarker{
				CompactorID: "foo",
				VisitTime:   now,
				Version:     VisitMarkerVersion1,
			},
			blocks: []*metadata.Meta{
				{
					BlockMeta: tsdb.BlockMeta{
						ULID: ulid0,
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID: ulid1,
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID: ulid2,
					},
				},
			},
		},
		{
			name: "write visit marker succeeded 2",
			visitMarker: BlockVisitMarker{
				CompactorID: "bar",
				VisitTime:   nowBefore1h,
				Version:     VisitMarkerVersion1,
			},
			blocks: []*metadata.Meta{
				{
					BlockMeta: tsdb.BlockMeta{
						ULID: ulid0,
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID: ulid1,
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID: ulid2,
					},
				},
			},
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			ctx := context.Background()
			dummyCounter := prometheus.NewCounter(prometheus.CounterOpts{})
			bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
			logger := log.NewNopLogger()
			markBlocksVisited(ctx, bkt, logger, tcase.blocks, tcase.visitMarker, dummyCounter)
			for _, meta := range tcase.blocks {
				res, err := ReadBlockVisitMarker(ctx, objstore.WithNoopInstr(bkt), logger, meta.ULID.String(), tcase.visitMarker.PartitionID, dummyCounter)
				require.NoError(t, err)
				require.Equal(t, tcase.visitMarker, *res)
			}
		})
	}
}

func TestMarkBlockVisitedHeartBeat(t *testing.T) {
	partitionedGroupID := uint32(12345)
	partitionID := 0
	compactorID := "test-compactor"
	for _, tcase := range []struct {
		name           string
		isCancelled    bool
		compactionErr  error
		expectedStatus VisitStatus
	}{
		{
			name:           "heart beat got cancelled",
			isCancelled:    true,
			compactionErr:  nil,
			expectedStatus: Pending,
		},
		{
			name:           "heart beat complete without error",
			isCancelled:    false,
			compactionErr:  nil,
			expectedStatus: Completed,
		},
		{
			name:           "heart beat stopped due to compaction error",
			isCancelled:    false,
			compactionErr:  fmt.Errorf("some compaction failure"),
			expectedStatus: Pending,
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			ulid0 := ulid.MustNew(uint64(time.Now().UnixMilli()+0), nil)
			ulid1 := ulid.MustNew(uint64(time.Now().UnixMilli()+1), nil)
			blocks := []*metadata.Meta{
				{
					BlockMeta: tsdb.BlockMeta{
						ULID: ulid0,
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID: ulid1,
					},
				},
			}
			ctx, cancel := context.WithCancel(context.Background())
			dummyCounter := prometheus.NewCounter(prometheus.CounterOpts{})
			bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
			logger := log.NewNopLogger()
			errChan := make(chan error, 1)
			go markBlocksVisitedHeartBeat(ctx, objstore.WithNoopInstr(bkt), logger, blocks, partitionedGroupID, partitionID, compactorID, time.Second, dummyCounter, errChan)
			time.Sleep(2 * time.Second)
			if tcase.isCancelled {
				cancel()
			} else {
				errChan <- tcase.compactionErr
				defer cancel()
			}
			time.Sleep(2 * time.Second)
			for _, meta := range blocks {
				res, err := ReadBlockVisitMarker(context.Background(), objstore.WithNoopInstr(bkt), logger, meta.ULID.String(), partitionID, dummyCounter)
				require.NoError(t, err)
				require.Equal(t, tcase.expectedStatus, res.Status)
			}
		})
	}
}
