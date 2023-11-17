package compactor

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	cortex_testutil "github.com/cortexproject/cortex/pkg/storage/tsdb/testutil"
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
				Version:    VisitMarkerVersion1,
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
				RangeStart:   rangeStart,
				RangeEnd:     rangeEnd,
				CreationTime: time.Now().Unix(),
				Version:      VisitMarkerVersion1,
			},
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			ctx := context.Background()
			dummyReadCounter := prometheus.NewCounter(prometheus.CounterOpts{})
			dummyWriteCounter := prometheus.NewCounter(prometheus.CounterOpts{})
			testBkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
			bkt := objstore.WithNoopInstr(testBkt)
			logger := log.NewNopLogger()
			writeRes, err := UpdatePartitionedGroupInfo(ctx, bkt, logger, tcase.partitionedGroupInfo, dummyReadCounter, dummyWriteCounter)
			require.NoError(t, err)
			require.Equal(t, tcase.partitionedGroupInfo, *writeRes)
			readRes, err := ReadPartitionedGroupInfo(ctx, bkt, logger, tcase.partitionedGroupInfo.PartitionedGroupID, dummyReadCounter)
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
		Version:    VisitMarkerVersion1,
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
