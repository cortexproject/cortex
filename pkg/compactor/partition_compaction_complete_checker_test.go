package compactor

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/compact"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
)

func TestPartitionCompactionCompleteChecker(t *testing.T) {
	ulid0 := ulid.MustNew(0, nil)
	ulid1 := ulid.MustNew(1, nil)
	ulid2 := ulid.MustNew(2, nil)

	rangeStart := (1 * time.Hour).Milliseconds()
	rangeEnd := (2 * time.Hour).Milliseconds()
	partitionedGroupID := uint32(12345)
	compactorID := "compactor1"
	timeBefore1h := time.Now().Add(-1 * time.Hour).Unix()
	timeNow := time.Now().Unix()

	for _, tcase := range []struct {
		name                 string
		partitionedGroupInfo PartitionedGroupInfo
		blocks               map[ulid.ULID]struct {
			expectComplete bool
		}
		visitMarkers []PartitionVisitMarker
	}{
		{
			name: "all partitions are complete 1",
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
			blocks: map[ulid.ULID]struct {
				expectComplete bool
			}{
				ulid0: {
					expectComplete: true,
				},
				ulid1: {
					expectComplete: true,
				},
				ulid2: {
					expectComplete: true,
				},
			},
			visitMarkers: []PartitionVisitMarker{
				{
					Status:             Completed,
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					CompactorID:        compactorID,
					VisitTime:          timeBefore1h,
					Version:            PartitionVisitMarkerVersion1,
				},
				{
					Status:             Completed,
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        1,
					CompactorID:        compactorID,
					VisitTime:          timeBefore1h,
					Version:            PartitionVisitMarkerVersion1,
				},
			},
		},
		{
			name: "all partitions are complete 2",
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
			blocks: map[ulid.ULID]struct {
				expectComplete bool
			}{
				ulid0: {
					expectComplete: true,
				},
				ulid1: {
					expectComplete: true,
				},
				ulid2: {
					expectComplete: true,
				},
			},
			visitMarkers: []PartitionVisitMarker{
				{
					Status:             Completed,
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					CompactorID:        compactorID,
					VisitTime:          timeNow,
					Version:            PartitionVisitMarkerVersion1,
				},
				{
					Status:             Completed,
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        1,
					CompactorID:        compactorID,
					VisitTime:          timeNow,
					Version:            PartitionVisitMarkerVersion1,
				},
				{
					Status:             Completed,
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        2,
					CompactorID:        compactorID,
					VisitTime:          timeNow,
					Version:            PartitionVisitMarkerVersion1,
				},
			},
		},
		{
			name: "not all partitions are complete 1",
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
							ulid0,
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
			blocks: map[ulid.ULID]struct {
				expectComplete bool
			}{
				ulid0: {
					expectComplete: false,
				},
				ulid1: {
					expectComplete: true,
				},
				ulid2: {
					expectComplete: true,
				},
			},
			visitMarkers: []PartitionVisitMarker{
				{
					Status:             Pending,
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					CompactorID:        compactorID,
					VisitTime:          timeBefore1h,
					Version:            PartitionVisitMarkerVersion1,
				},
				{
					Status:             Completed,
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        1,
					CompactorID:        compactorID,
					VisitTime:          timeBefore1h,
					Version:            PartitionVisitMarkerVersion1,
				},
				{
					Status:             Completed,
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        2,
					CompactorID:        compactorID,
					VisitTime:          timeBefore1h,
					Version:            PartitionVisitMarkerVersion1,
				},
			},
		},
		{
			name: "not all partitions are complete 2",
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
							ulid0,
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
			blocks: map[ulid.ULID]struct {
				expectComplete bool
			}{
				ulid0: {
					expectComplete: false,
				},
				ulid1: {
					expectComplete: false,
				},
				ulid2: {
					expectComplete: true,
				},
			},
			visitMarkers: []PartitionVisitMarker{
				{
					Status:             Completed,
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        0,
					CompactorID:        compactorID,
					VisitTime:          timeNow,
					Version:            PartitionVisitMarkerVersion1,
				},
				{
					Status:             Pending,
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        1,
					CompactorID:        compactorID,
					VisitTime:          timeNow,
					Version:            PartitionVisitMarkerVersion1,
				},
				{
					Status:             Completed,
					PartitionedGroupID: partitionedGroupID,
					PartitionID:        2,
					CompactorID:        compactorID,
					VisitTime:          timeNow,
					Version:            PartitionVisitMarkerVersion1,
				},
			},
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			bkt := &bucket.ClientMock{}
			partitionedGroupInfoFileContent, _ := json.Marshal(tcase.partitionedGroupInfo)
			bkt.MockGet(GetPartitionedGroupFile(partitionedGroupID), string(partitionedGroupInfoFileContent), nil)
			checker := NewPartitionCompactionBlockDeletableChecker(
				context.Background(),
				objstore.WithNoopInstr(bkt),
				log.NewNopLogger(),
				prometheus.NewCounter(prometheus.CounterOpts{}),
				prometheus.NewCounter(prometheus.CounterOpts{}),
			)
			group := compact.Group{}
			// set partitionID to -1, so it will go through all partitionIDs when checking
			group.SetExtensions(&CortexMetaExtensions{
				PartitionInfo: &PartitionInfo{
					PartitionedGroupID: tcase.partitionedGroupInfo.PartitionedGroupID,
					PartitionCount:     tcase.partitionedGroupInfo.PartitionCount,
					PartitionID:        -1,
				},
			})
			for _, visitMarker := range tcase.visitMarkers {
				visitMarkerFileContent, _ := json.Marshal(visitMarker)
				bkt.MockGet(GetPartitionVisitMarkerFile(visitMarker.PartitionedGroupID, visitMarker.PartitionID), string(visitMarkerFileContent), nil)
			}
			for blockID, blockTCase := range tcase.blocks {
				require.Equal(t, blockTCase.expectComplete, checker.CanDelete(&group, blockID))
			}
		})
	}
}
