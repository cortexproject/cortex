package compactor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	cortex_testutil "github.com/cortexproject/cortex/pkg/storage/tsdb/testutil"
)

func TestMarkPartitionVisited(t *testing.T) {
	now := time.Now().Unix()
	nowBefore1h := time.Now().Add(-1 * time.Hour).Unix()
	partitionedGroupID1 := uint32(1234)
	partitionedGroupID2 := uint32(5678)
	for _, tcase := range []struct {
		name               string
		visitMarker        PartitionVisitMarker
		partitionedGroupID uint32
	}{
		{
			name: "write visit marker succeeded",
			visitMarker: PartitionVisitMarker{
				CompactorID:        "foo",
				PartitionedGroupID: partitionedGroupID1,
				PartitionID:        0,
				VisitTime:          now,
				Version:            PartitionVisitMarkerVersion1,
			},
			partitionedGroupID: partitionedGroupID1,
		},
		{
			name: "write visit marker succeeded 2",
			visitMarker: PartitionVisitMarker{
				CompactorID:        "bar",
				PartitionedGroupID: partitionedGroupID2,
				PartitionID:        3,
				VisitTime:          nowBefore1h,
				Version:            PartitionVisitMarkerVersion1,
			},
			partitionedGroupID: partitionedGroupID2,
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			ctx := context.Background()
			dummyCounter := prometheus.NewCounter(prometheus.CounterOpts{})
			bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
			logger := log.NewNopLogger()
			markPartitionVisited(ctx, bkt, logger, tcase.visitMarker, dummyCounter)
			res, err := ReadPartitionVisitMarker(ctx, objstore.WithNoopInstr(bkt), logger, tcase.partitionedGroupID, tcase.visitMarker.PartitionID, dummyCounter)
			require.NoError(t, err)
			require.Equal(t, tcase.visitMarker, *res)
		})
	}
}

func TestMarkPartitionVisitedHeartBeat(t *testing.T) {
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
			ctx, cancel := context.WithCancel(context.Background())
			dummyCounter := prometheus.NewCounter(prometheus.CounterOpts{})
			bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
			logger := log.NewNopLogger()
			errChan := make(chan error, 1)
			go markPartitionVisitedHeartBeat(ctx, objstore.WithNoopInstr(bkt), logger, partitionedGroupID, partitionID, compactorID, time.Second, dummyCounter, errChan)
			time.Sleep(2 * time.Second)
			if tcase.isCancelled {
				cancel()
			} else {
				errChan <- tcase.compactionErr
				defer cancel()
			}
			time.Sleep(2 * time.Second)
			res, err := ReadPartitionVisitMarker(context.Background(), objstore.WithNoopInstr(bkt), logger, partitionedGroupID, partitionID, dummyCounter)
			require.NoError(t, err)
			require.Equal(t, tcase.expectedStatus, res.Status)
		})
	}
}
