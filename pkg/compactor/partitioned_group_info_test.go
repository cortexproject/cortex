package compactor

import (
	"context"
	"encoding/json"
	"path"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_testutil "github.com/cortexproject/cortex/pkg/util/testutil"
)

func TestPartitionedGroupInfo(t *testing.T) {
	ulid0 := ulid.MustNew(0, nil)
	ulid1 := ulid.MustNew(1, nil)
	ulid2 := ulid.MustNew(2, nil)
	rangeStart := (1 * time.Hour).Milliseconds()
	rangeEnd := (2 * time.Hour).Milliseconds()
	partitionedGroupID := uint32(12345)
	for _, tcase := range []struct {
		name                 string
		partitionedGroupInfo PartitionedGroupInfo
	}{
		{
			name: "write partitioned group info 1",
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID: partitionedGroupID,
				PartitionCount:     2,
				Partitions: []Partition{
					{
						PartitionID: 0,
						Blocks: []ulid.ULID{
							ulid0,
							ulid1,
						},
					},
					{
						PartitionID: 1,
						Blocks: []ulid.ULID{
							ulid0,
							ulid2,
						},
					},
				},
				RangeStart: rangeStart,
				RangeEnd:   rangeEnd,
				Version:    PartitionedGroupInfoVersion1,
			},
		},
		{
			name: "write partitioned group info 2",
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID: partitionedGroupID,
				PartitionCount:     3,
				Partitions: []Partition{
					{
						PartitionID: 0,
						Blocks: []ulid.ULID{
							ulid0,
						},
					},
					{
						PartitionID: 1,
						Blocks: []ulid.ULID{
							ulid1,
						},
					},
					{
						PartitionID: 2,
						Blocks: []ulid.ULID{
							ulid2,
						},
					},
				},
				RangeStart: rangeStart,
				RangeEnd:   rangeEnd,
				Version:    PartitionedGroupInfoVersion1,
			},
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			ctx := context.Background()
			testBkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
			bkt := objstore.WithNoopInstr(testBkt)
			logger := log.NewNopLogger()
			writeRes, err := UpdatePartitionedGroupInfo(ctx, bkt, logger, tcase.partitionedGroupInfo)
			tcase.partitionedGroupInfo.CreationTime = writeRes.CreationTime
			require.NoError(t, err)
			require.Equal(t, tcase.partitionedGroupInfo, *writeRes)
			readRes, err := ReadPartitionedGroupInfo(ctx, bkt, logger, tcase.partitionedGroupInfo.PartitionedGroupID)
			require.NoError(t, err)
			require.Equal(t, tcase.partitionedGroupInfo, *readRes)
		})
	}
}

func TestGetPartitionIDsByBlock(t *testing.T) {
	ulid0 := ulid.MustNew(0, nil)
	ulid1 := ulid.MustNew(1, nil)
	ulid2 := ulid.MustNew(2, nil)
	ulid3 := ulid.MustNew(3, nil)
	partitionedGroupInfo := PartitionedGroupInfo{
		PartitionedGroupID: uint32(12345),
		PartitionCount:     3,
		Partitions: []Partition{
			{
				PartitionID: 0,
				Blocks: []ulid.ULID{
					ulid0,
					ulid1,
				},
			},
			{
				PartitionID: 1,
				Blocks: []ulid.ULID{
					ulid0,
					ulid2,
				},
			},
			{
				PartitionID: 2,
				Blocks: []ulid.ULID{
					ulid0,
					ulid1,
					ulid2,
					ulid3,
				},
			},
		},
		RangeStart: (1 * time.Hour).Milliseconds(),
		RangeEnd:   (2 * time.Hour).Milliseconds(),
		Version:    PartitionedGroupInfoVersion1,
	}

	res0 := partitionedGroupInfo.getPartitionIDsByBlock(ulid0)
	require.Equal(t, 3, len(res0))
	require.Contains(t, res0, 0)
	require.Contains(t, res0, 1)
	require.Contains(t, res0, 2)

	res1 := partitionedGroupInfo.getPartitionIDsByBlock(ulid1)
	require.Equal(t, 2, len(res1))
	require.Contains(t, res1, 0)
	require.Contains(t, res1, 2)

	res2 := partitionedGroupInfo.getPartitionIDsByBlock(ulid2)
	require.Equal(t, 2, len(res2))
	require.Contains(t, res2, 1)
	require.Contains(t, res2, 2)

	res3 := partitionedGroupInfo.getPartitionIDsByBlock(ulid3)
	require.Equal(t, 1, len(res3))
	require.Contains(t, res3, 2)
}

func TestGetPartitionedGroupStatus(t *testing.T) {
	ulid0 := ulid.MustNew(0, nil)
	ulid1 := ulid.MustNew(1, nil)
	ulid2 := ulid.MustNew(2, nil)
	partitionedGroupID := uint32(1234)
	for _, tcase := range []struct {
		name                  string
		expectedResult        PartitionedGroupStatus
		partitionedGroupInfo  PartitionedGroupInfo
		partitionVisitMarkers []partitionVisitMarker
		deletedBlock          map[ulid.ULID]bool
		noCompactBlock        map[ulid.ULID]struct{}
	}{
		{
			name: "test one partition is not visited and contains block marked for deletion",
			expectedResult: PartitionedGroupStatus{
				CanDelete:            true,
				IsCompleted:          false,
				VisitMarkersToDelete: []VisitMarker{},
				PendingOrFailedPartitions: []Partition{
					{
						PartitionID: 1,
						Blocks: []ulid.ULID{
							ulid0,
							ulid2,
						},
					},
				},
			},
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID: partitionedGroupID,
				PartitionCount:     2,
				Partitions: []Partition{
					{
						PartitionID: 0,
						Blocks: []ulid.ULID{
							ulid0,
							ulid1,
						},
					},
					{
						PartitionID: 1,
						Blocks: []ulid.ULID{
							ulid0,
							ulid2,
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(-10 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion1,
			},
			partitionVisitMarkers: []partitionVisitMarker{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
			},
			deletedBlock: map[ulid.ULID]bool{
				ulid0: true,
			},
		},
		{
			name: "test one partition is pending and contains block marked for deletion",
			expectedResult: PartitionedGroupStatus{
				CanDelete:            true,
				IsCompleted:          false,
				VisitMarkersToDelete: []VisitMarker{},
				PendingOrFailedPartitions: []Partition{
					{
						PartitionID: 1,
						Blocks: []ulid.ULID{
							ulid0,
							ulid2,
						},
					},
				},
			},
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID: partitionedGroupID,
				PartitionCount:     2,
				Partitions: []Partition{
					{
						PartitionID: 0,
						Blocks: []ulid.ULID{
							ulid0,
							ulid1,
						},
					},
					{
						PartitionID: 1,
						Blocks: []ulid.ULID{
							ulid0,
							ulid2,
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(-10 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion1,
			},
			partitionVisitMarkers: []partitionVisitMarker{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        1,
					Status:             Pending,
					VisitTime:          time.Now().Add(-5 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
			},
			deletedBlock: map[ulid.ULID]bool{
				ulid0: true,
			},
		},
		{
			name: "test one partition is completed and one partition is under visiting",
			expectedResult: PartitionedGroupStatus{
				CanDelete:                 false,
				IsCompleted:               false,
				VisitMarkersToDelete:      []VisitMarker{},
				PendingOrFailedPartitions: []Partition{},
			},
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID: partitionedGroupID,
				PartitionCount:     2,
				Partitions: []Partition{
					{
						PartitionID: 0,
						Blocks: []ulid.ULID{
							ulid0,
							ulid1,
						},
					},
					{
						PartitionID: 1,
						Blocks: []ulid.ULID{
							ulid0,
							ulid2,
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(-10 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion1,
			},
			partitionVisitMarkers: []partitionVisitMarker{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        1,
					Status:             Pending,
					VisitTime:          time.Now().Add(time.Second).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
			},
			deletedBlock: map[ulid.ULID]bool{
				ulid0: false,
			},
		},
		{
			name: "test one partition is pending expired",
			expectedResult: PartitionedGroupStatus{
				CanDelete:            false,
				IsCompleted:          false,
				VisitMarkersToDelete: []VisitMarker{},
				PendingOrFailedPartitions: []Partition{
					{
						PartitionID: 0,
						Blocks: []ulid.ULID{
							ulid0,
							ulid1,
						},
					},
					{
						PartitionID: 1,
						Blocks: []ulid.ULID{
							ulid0,
							ulid2,
						},
					},
				},
			},
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID: partitionedGroupID,
				PartitionCount:     2,
				Partitions: []Partition{
					{
						PartitionID: 0,
						Blocks: []ulid.ULID{
							ulid0,
							ulid1,
						},
					},
					{
						PartitionID: 1,
						Blocks: []ulid.ULID{
							ulid0,
							ulid2,
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(-10 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion1,
			},
			partitionVisitMarkers: []partitionVisitMarker{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        1,
					Status:             Pending,
					VisitTime:          time.Now().Add(-5 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
			},
			deletedBlock: map[ulid.ULID]bool{},
		},
		{
			name: "test one partition is complete with one block deleted and one partition is not visited with no blocks deleted",
			expectedResult: PartitionedGroupStatus{
				CanDelete:            false,
				IsCompleted:          false,
				VisitMarkersToDelete: []VisitMarker{},
				PendingOrFailedPartitions: []Partition{
					{
						PartitionID: 1,
						Blocks: []ulid.ULID{
							ulid0,
							ulid2,
						},
					},
				},
			},
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID: partitionedGroupID,
				PartitionCount:     2,
				Partitions: []Partition{
					{
						PartitionID: 0,
						Blocks: []ulid.ULID{
							ulid0,
							ulid1,
						},
					},
					{
						PartitionID: 1,
						Blocks: []ulid.ULID{
							ulid0,
							ulid2,
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(-10 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion1,
			},
			partitionVisitMarkers: []partitionVisitMarker{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
			},
			deletedBlock: map[ulid.ULID]bool{
				ulid1: true,
			},
		},
		{
			name: "test one partition is complete and one partition is failed with no blocks deleted",
			expectedResult: PartitionedGroupStatus{
				CanDelete:            false,
				IsCompleted:          false,
				VisitMarkersToDelete: []VisitMarker{},
				PendingOrFailedPartitions: []Partition{
					{
						PartitionID: 1,
						Blocks: []ulid.ULID{
							ulid0,
							ulid2,
						},
					},
				},
			},
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID: partitionedGroupID,
				PartitionCount:     2,
				Partitions: []Partition{
					{
						PartitionID: 0,
						Blocks: []ulid.ULID{
							ulid0,
							ulid1,
						},
					},
					{
						PartitionID: 1,
						Blocks: []ulid.ULID{
							ulid0,
							ulid2,
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(-10 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion1,
			},
			partitionVisitMarkers: []partitionVisitMarker{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        1,
					Status:             Failed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
			},
			deletedBlock: map[ulid.ULID]bool{},
		},
		{
			name: "test one partition is complete and one partition is failed one block deleted",
			expectedResult: PartitionedGroupStatus{
				CanDelete:            true,
				IsCompleted:          false,
				VisitMarkersToDelete: []VisitMarker{},
				PendingOrFailedPartitions: []Partition{
					{
						PartitionID: 1,
						Blocks: []ulid.ULID{
							ulid0,
							ulid2,
						},
					},
				},
			},
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID: partitionedGroupID,
				PartitionCount:     2,
				Partitions: []Partition{
					{
						PartitionID: 0,
						Blocks: []ulid.ULID{
							ulid0,
							ulid1,
						},
					},
					{
						PartitionID: 1,
						Blocks: []ulid.ULID{
							ulid0,
							ulid2,
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(-10 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion1,
			},
			partitionVisitMarkers: []partitionVisitMarker{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        1,
					Status:             Failed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
			},
			deletedBlock: map[ulid.ULID]bool{
				ulid2: true,
			},
		},
		{
			name: "test all partitions are complete",
			expectedResult: PartitionedGroupStatus{
				CanDelete:                 true,
				IsCompleted:               true,
				VisitMarkersToDelete:      []VisitMarker{},
				PendingOrFailedPartitions: []Partition{},
			},
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID: partitionedGroupID,
				PartitionCount:     2,
				Partitions: []Partition{
					{
						PartitionID: 0,
						Blocks: []ulid.ULID{
							ulid0,
							ulid1,
						},
					},
					{
						PartitionID: 1,
						Blocks: []ulid.ULID{
							ulid0,
							ulid2,
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(-10 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion1,
			},
			partitionVisitMarkers: []partitionVisitMarker{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        1,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
			},
			deletedBlock: map[ulid.ULID]bool{
				ulid2: true,
			},
		},
		{
			name: "test partitioned group created after visit marker",
			expectedResult: PartitionedGroupStatus{
				CanDelete:   false,
				IsCompleted: false,
				VisitMarkersToDelete: []VisitMarker{
					&partitionVisitMarker{
						PartitionedGroupID: partitionedGroupID,
						PartitionID:        0,
						Status:             Completed,
						VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
						Version:            PartitionVisitMarkerVersion1,
					},
					&partitionVisitMarker{
						PartitionedGroupID: partitionedGroupID,
						PartitionID:        1,
						Status:             Completed,
						VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
						Version:            PartitionVisitMarkerVersion1,
					},
				},
				PendingOrFailedPartitions: []Partition{},
			},
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID: partitionedGroupID,
				PartitionCount:     2,
				Partitions: []Partition{
					{
						PartitionID: 0,
						Blocks: []ulid.ULID{
							ulid0,
							ulid1,
						},
					},
					{
						PartitionID: 1,
						Blocks: []ulid.ULID{
							ulid0,
							ulid2,
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(1 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion1,
			},
			partitionVisitMarkers: []partitionVisitMarker{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        1,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
			},
			deletedBlock: map[ulid.ULID]bool{},
		},
		{
			name: "test one partition is in progress not expired and contains block marked for deletion",
			expectedResult: PartitionedGroupStatus{
				CanDelete:                 false,
				IsCompleted:               false,
				VisitMarkersToDelete:      []VisitMarker{},
				PendingOrFailedPartitions: []Partition{},
			},
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID: partitionedGroupID,
				PartitionCount:     2,
				Partitions: []Partition{
					{
						PartitionID: 0,
						Blocks: []ulid.ULID{
							ulid0,
							ulid1,
						},
					},
					{
						PartitionID: 1,
						Blocks: []ulid.ULID{
							ulid0,
							ulid2,
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(-10 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion1,
			},
			partitionVisitMarkers: []partitionVisitMarker{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        1,
					Status:             InProgress,
					VisitTime:          time.Now().Add(time.Second).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
			},
			deletedBlock: map[ulid.ULID]bool{
				ulid0: true,
			},
		},
		{
			name: "test one partition is not visited and contains block with no compact mark",
			expectedResult: PartitionedGroupStatus{
				CanDelete:            true,
				IsCompleted:          false,
				VisitMarkersToDelete: []VisitMarker{},
				PendingOrFailedPartitions: []Partition{
					{
						PartitionID: 1,
						Blocks: []ulid.ULID{
							ulid0,
							ulid2,
						},
					},
				},
			},
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID: partitionedGroupID,
				PartitionCount:     2,
				Partitions: []Partition{
					{
						PartitionID: 0,
						Blocks: []ulid.ULID{
							ulid0,
							ulid1,
						},
					},
					{
						PartitionID: 1,
						Blocks: []ulid.ULID{
							ulid0,
							ulid2,
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(-10 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion1,
			},
			partitionVisitMarkers: []partitionVisitMarker{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
			},
			noCompactBlock: map[ulid.ULID]struct{}{
				ulid0: {},
			},
		},
		{
			name: "test one partition is expired and contains block with no compact mark",
			expectedResult: PartitionedGroupStatus{
				CanDelete:            true,
				IsCompleted:          false,
				VisitMarkersToDelete: []VisitMarker{},
				PendingOrFailedPartitions: []Partition{
					{
						PartitionID: 1,
						Blocks: []ulid.ULID{
							ulid0,
							ulid2,
						},
					},
				},
			},
			partitionedGroupInfo: PartitionedGroupInfo{
				PartitionedGroupID: partitionedGroupID,
				PartitionCount:     2,
				Partitions: []Partition{
					{
						PartitionID: 0,
						Blocks: []ulid.ULID{
							ulid0,
							ulid1,
						},
					},
					{
						PartitionID: 1,
						Blocks: []ulid.ULID{
							ulid0,
							ulid2,
						},
					},
				},
				RangeStart:   (1 * time.Hour).Milliseconds(),
				RangeEnd:     (2 * time.Hour).Milliseconds(),
				CreationTime: time.Now().Add(-10 * time.Minute).Unix(),
				Version:      PartitionedGroupInfoVersion1,
			},
			partitionVisitMarkers: []partitionVisitMarker{
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					Status:             Completed,
					VisitTime:          time.Now().Add(-2 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
				{
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        1,
					Status:             InProgress,
					VisitTime:          time.Now().Add(-10 * time.Minute).Unix(),
					Version:            PartitionVisitMarkerVersion1,
				},
			},
			noCompactBlock: map[ulid.ULID]struct{}{
				ulid0: {},
			},
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			bucketClient := &bucket.ClientMock{}
			for _, partitionVisitMarker := range tcase.partitionVisitMarkers {
				content, _ := json.Marshal(partitionVisitMarker)
				bucketClient.MockGet(partitionVisitMarker.GetVisitMarkerFilePath(), string(content), nil)
			}

			for _, partition := range tcase.partitionedGroupInfo.Partitions {
				for _, blockID := range partition.Blocks {
					metaPath := path.Join(blockID.String(), metadata.MetaFilename)
					noCompactPath := path.Join(blockID.String(), metadata.NoCompactMarkFilename)
					deletionMarkerPath := path.Join(blockID.String(), metadata.DeletionMarkFilename)
					if hasDeletionMarker, ok := tcase.deletedBlock[blockID]; ok {
						if hasDeletionMarker {
							bucketClient.MockExists(metaPath, true, nil)
							bucketClient.MockExists(deletionMarkerPath, true, nil)
						} else {
							bucketClient.MockExists(metaPath, false, nil)
						}
					} else {
						bucketClient.MockExists(metaPath, true, nil)
						bucketClient.MockExists(deletionMarkerPath, false, nil)
					}
					if _, ok := tcase.noCompactBlock[blockID]; ok {
						bucketClient.MockExists(noCompactPath, true, nil)
					} else {
						bucketClient.MockExists(noCompactPath, false, nil)
					}
				}
			}
			bucketClient.MockGet(mock.Anything, "", nil)

			ctx := context.Background()
			logger := log.NewNopLogger()
			result := tcase.partitionedGroupInfo.getPartitionedGroupStatus(ctx, bucketClient, 60*time.Second, logger)
			require.Equal(t, tcase.expectedResult.CanDelete, result.CanDelete)
			require.Equal(t, tcase.expectedResult.IsCompleted, result.IsCompleted)
			require.Equal(t, len(tcase.expectedResult.PendingOrFailedPartitions), len(result.PendingOrFailedPartitions))
			for _, partition := range result.PendingOrFailedPartitions {
				require.Contains(t, tcase.expectedResult.PendingOrFailedPartitions, partition)
			}
			require.Equal(t, len(tcase.expectedResult.VisitMarkersToDelete), len(result.VisitMarkersToDelete))
			for _, visitMarker := range result.VisitMarkersToDelete {
				require.Contains(t, tcase.expectedResult.VisitMarkersToDelete, visitMarker)
			}
		})
	}
}

func TestUpdatePartitionedGroupInfo_ErrorHandling(t *testing.T) {
	partitionedGroupID := uint32(12345)
	partitionedGroupInfo := PartitionedGroupInfo{
		PartitionedGroupID: partitionedGroupID,
		PartitionCount:     1,
		Partitions: []Partition{
			{
				PartitionID: 0,
				Blocks:      []ulid.ULID{ulid.MustNew(0, nil)},
			},
		},
		RangeStart: (1 * time.Hour).Milliseconds(),
		RangeEnd:   (2 * time.Hour).Milliseconds(),
		Version:    PartitionedGroupInfoVersion1,
	}

	t.Run("should return error when ReadPartitionedGroupInfo fails with non-NotFound error", func(t *testing.T) {
		ctx := context.Background()
		testBkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
		logger := log.NewNopLogger()

		// Wrap the bucket to inject a Get failure on the partitioned group file path.
		// This simulates S3 throttling or transient errors.
		failingBkt := &cortex_testutil.MockBucketFailure{
			Bucket: testBkt,
			GetFailures: map[string]error{
				GetPartitionedGroupFile(partitionedGroupID): errors.New("simulated S3 throttling error"),
			},
		}

		result, err := UpdatePartitionedGroupInfo(ctx, failingBkt, logger, partitionedGroupInfo)
		require.Error(t, err)
		require.Nil(t, result)
		require.Contains(t, err.Error(), "unable to check existing partitioned group info")
	})

	t.Run("should create partitioned group info when ReadPartitionedGroupInfo returns NotFound", func(t *testing.T) {
		ctx := context.Background()
		testBkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
		bkt := objstore.WithNoopInstr(testBkt)
		logger := log.NewNopLogger()

		// No pre-existing file — ReadPartitionedGroupInfo should return ErrorPartitionedGroupInfoNotFound,
		// and UpdatePartitionedGroupInfo should proceed to create the file.
		result, err := UpdatePartitionedGroupInfo(ctx, bkt, logger, partitionedGroupInfo)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, partitionedGroupID, result.PartitionedGroupID)
		require.Greater(t, result.CreationTime, int64(0))

		// Verify it was actually written to the bucket.
		readResult, err := ReadPartitionedGroupInfo(ctx, bkt, logger, partitionedGroupID)
		require.NoError(t, err)
		require.Equal(t, result.PartitionedGroupID, readResult.PartitionedGroupID)
		require.Equal(t, result.CreationTime, readResult.CreationTime)
	})

	t.Run("should return existing partitioned group info when it already exists", func(t *testing.T) {
		ctx := context.Background()
		testBkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
		bkt := objstore.WithNoopInstr(testBkt)
		logger := log.NewNopLogger()

		// First, create a partitioned group info.
		firstResult, err := UpdatePartitionedGroupInfo(ctx, bkt, logger, partitionedGroupInfo)
		require.NoError(t, err)
		require.NotNil(t, firstResult)

		// Try to update again with the same partitioned group ID — should return the existing one.
		modifiedInfo := partitionedGroupInfo
		modifiedInfo.PartitionCount = 5 // Different value to prove we get the original back
		secondResult, err := UpdatePartitionedGroupInfo(ctx, bkt, logger, modifiedInfo)
		require.NoError(t, err)
		require.NotNil(t, secondResult)
		// Should return the original, not the modified one.
		require.Equal(t, firstResult.PartitionCount, secondResult.PartitionCount)
		require.Equal(t, firstResult.CreationTime, secondResult.CreationTime)
	})

	t.Run("should not overwrite existing file when S3 Get fails", func(t *testing.T) {
		ctx := context.Background()
		testBkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
		bkt := objstore.WithNoopInstr(testBkt)
		logger := log.NewNopLogger()

		// First, create a partitioned group info successfully.
		firstResult, err := UpdatePartitionedGroupInfo(ctx, bkt, logger, partitionedGroupInfo)
		require.NoError(t, err)
		require.NotNil(t, firstResult)
		originalCreationTime := firstResult.CreationTime

		// Now wrap the bucket to simulate S3 throttling on the existence check.
		failingBkt := &cortex_testutil.MockBucketFailure{
			Bucket: testBkt,
			GetFailures: map[string]error{
				GetPartitionedGroupFile(partitionedGroupID): errors.New("simulated S3 throttling error"),
			},
		}

		// Attempt to update — this should fail, NOT overwrite.
		modifiedInfo := partitionedGroupInfo
		modifiedInfo.CreationTime = time.Now().Unix() + 9999 // A clearly different creation time
		secondResult, err := UpdatePartitionedGroupInfo(ctx, failingBkt, logger, modifiedInfo)
		require.Error(t, err)
		require.Nil(t, secondResult)

		// Verify the original file was NOT overwritten.
		readResult, err := ReadPartitionedGroupInfo(ctx, bkt, logger, partitionedGroupID)
		require.NoError(t, err)
		require.Equal(t, originalCreationTime, readResult.CreationTime)
	})
}
