package purger

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/purger/purgeplan"
	"github.com/cortexproject/cortex/pkg/chunk/testutils"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

const (
	userID       = "userID"
	modelTimeDay = model.Time(millisecondPerDay)
)

func setupStoresAndPurger(t *testing.T) (*chunk.DeleteStore, chunk.Store, chunk.StorageClient, *DataPurger) {
	deleteStore, err := testutils.SetupTestDeleteStore()
	require.NoError(t, err)

	chunkStore, err := testutils.SetupTestChunkStore()
	require.NoError(t, err)

	storageClient, err := testutils.SetupTestObjectStore()
	require.NoError(t, err)

	var cfg DataPurgerConfig
	flagext.DefaultValues(&cfg)

	dataPurger, err := NewDataPurger(cfg, deleteStore, chunkStore, storageClient)
	require.NoError(t, err)

	return deleteStore, chunkStore, storageClient, dataPurger
}

func buildChunks(from, through model.Time) ([]chunk.Chunk, error) {
	var chunks []chunk.Chunk
	for ; from < through; from = from.Add(time.Hour) {
		// creating 5 chunks per hour
		_, testChunks, err := testutils.CreateChunks(0, 1, from, from.Add(time.Hour))
		if err != nil {
			return nil, err
		}

		chunks = append(chunks, testChunks...)
	}

	return chunks, nil
}

func TestDataPurger_BuildPlan(t *testing.T) {
	for _, tc := range []struct {
		name                              string
		chunkStoreDataInterval            model.Interval
		deleteRequestInterval             model.Interval
		expectedNumberOfPlans             int
		numChunksToDelete                 int
		firstChunkPartialDeletionInterval *purgeplan.Interval
		lastChunkPartialDeletionInterval  *purgeplan.Interval
	}{
		{
			name:                   "deleting whole hour from a one hour data",
			chunkStoreDataInterval: model.Interval{End: model.Time(time.Hour / time.Millisecond)},
			deleteRequestInterval:  model.Interval{End: model.Time(time.Hour / time.Millisecond)},
			expectedNumberOfPlans:  1,
			numChunksToDelete:      1,
		},
		{
			name:                   "deleting half a day from a days data",
			chunkStoreDataInterval: model.Interval{End: modelTimeDay},
			deleteRequestInterval:  model.Interval{End: model.Time(millisecondPerDay / 2)},
			expectedNumberOfPlans:  1,
			numChunksToDelete:      12 + 1,
			lastChunkPartialDeletionInterval: &purgeplan.Interval{StartTimestampMs: int64(millisecondPerDay / 2),
				EndTimestampMs: int64(millisecondPerDay / 2)},
		},
		{
			name:                   "deleting a full day from 2 days data",
			chunkStoreDataInterval: model.Interval{End: modelTimeDay * 2},
			deleteRequestInterval:  model.Interval{End: modelTimeDay},
			expectedNumberOfPlans:  1,
			numChunksToDelete:      24 + 1,
			lastChunkPartialDeletionInterval: &purgeplan.Interval{StartTimestampMs: millisecondPerDay,
				EndTimestampMs: millisecondPerDay},
		},
		{
			name:                   "deleting 2 days partially from 2 days data",
			chunkStoreDataInterval: model.Interval{End: modelTimeDay * 2},
			deleteRequestInterval: model.Interval{Start: model.Time(millisecondPerDay / 2),
				End: model.Time(millisecondPerDay + millisecondPerDay/2)},
			expectedNumberOfPlans: 2,
			numChunksToDelete:     24 + 2, // one chunk for each hour + start and end time touches chunk at boundary
			firstChunkPartialDeletionInterval: &purgeplan.Interval{StartTimestampMs: int64(millisecondPerDay / 2),
				EndTimestampMs: int64(millisecondPerDay / 2)},
			lastChunkPartialDeletionInterval: &purgeplan.Interval{StartTimestampMs: millisecondPerDay + millisecondPerDay/2,
				EndTimestampMs: millisecondPerDay + millisecondPerDay/2},
		},
		{
			name:                   "deleting 2 days partially, not aligned with hour, from 2 days data",
			chunkStoreDataInterval: model.Interval{End: modelTimeDay * 2},
			deleteRequestInterval: model.Interval{Start: model.Time(millisecondPerDay / 2).Add(time.Minute),
				End: model.Time(millisecondPerDay + millisecondPerDay/2).Add(-time.Minute)},
			expectedNumberOfPlans: 2,
			numChunksToDelete:     24,
			firstChunkPartialDeletionInterval: &purgeplan.Interval{StartTimestampMs: int64(model.Time(millisecondPerDay / 2).Add(time.Minute)),
				EndTimestampMs: int64(model.Time(millisecondPerDay / 2).Add(time.Hour))},
			lastChunkPartialDeletionInterval: &purgeplan.Interval{StartTimestampMs: int64(model.Time(millisecondPerDay + millisecondPerDay/2).Add(-time.Hour)),
				EndTimestampMs: int64(model.Time(millisecondPerDay + millisecondPerDay/2).Add(-time.Minute))},
		},
		{
			name:                   "deleting data outside of period of existing data",
			chunkStoreDataInterval: model.Interval{End: modelTimeDay},
			deleteRequestInterval:  model.Interval{Start: model.Time(millisecondPerDay * 2), End: model.Time(millisecondPerDay * 3)},
			expectedNumberOfPlans:  1,
			numChunksToDelete:      0,
		},
		{
			name:                   "building multi-day chunk and deleting part of it from first day",
			chunkStoreDataInterval: model.Interval{Start: modelTimeDay.Add(-30 * time.Minute), End: modelTimeDay.Add(30 * time.Minute)},
			deleteRequestInterval:  model.Interval{Start: modelTimeDay.Add(-30 * time.Minute), End: modelTimeDay.Add(-15 * time.Minute)},
			expectedNumberOfPlans:  1,
			numChunksToDelete:      1,
			firstChunkPartialDeletionInterval: &purgeplan.Interval{StartTimestampMs: int64(modelTimeDay.Add(-30 * time.Minute)),
				EndTimestampMs: int64(modelTimeDay.Add(-15 * time.Minute))},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {

			deleteStore, chunkStore, storageClient, dataPurger := setupStoresAndPurger(t)

			chunks, err := buildChunks(tc.chunkStoreDataInterval.Start, tc.chunkStoreDataInterval.End)
			require.NoError(t, err)

			require.NoError(t, chunkStore.Put(context.Background(), chunks))

			err = deleteStore.AddDeleteRequest(context.Background(), userID, tc.deleteRequestInterval.Start,
				tc.deleteRequestInterval.End, []string{"foo"})
			require.NoError(t, err)

			deleteRequests, err := deleteStore.GetAllDeleteRequestsForUser(context.Background(), userID)
			require.NoError(t, err)

			deleteRequest := deleteRequests[0]
			err = dataPurger.buildDeletePlan(deleteRequest)
			require.NoError(t, err)
			planPath := fmt.Sprintf("%s:%s/", userID, deleteRequest.RequestID)

			plans, err := storageClient.List(context.Background(), planPath)
			require.NoError(t, err)
			require.Equal(t, tc.expectedNumberOfPlans, len(plans))

			numPlans := tc.expectedNumberOfPlans
			var nilPurgePlanInterval *purgeplan.Interval
			numChunks := 0

			chunkIDs := map[string]struct{}{}

			for i := range plans {
				deletePlan, err := dataPurger.getDeletePlan(context.Background(), userID, deleteRequest.RequestID, i)
				require.NoError(t, err)
				for _, chunksGroup := range deletePlan.ChunksGroup {
					numChunksInGroup := len(chunksGroup.Chunks)
					chunks := chunksGroup.Chunks
					numChunks += numChunksInGroup

					sort.Slice(chunks, func(i, j int) bool {
						chunkI, err := chunk.ParseExternalKey(userID, chunks[i].ID)
						require.NoError(t, err)

						chunkJ, err := chunk.ParseExternalKey(userID, chunks[j].ID)
						require.NoError(t, err)

						return chunkI.From < chunkJ.From
					})

					for j, chunkDetails := range chunksGroup.Chunks {
						chunkIDs[chunkDetails.ID] = struct{}{}
						if i == 0 && j == 0 && tc.firstChunkPartialDeletionInterval != nil {
							require.Equal(t, *tc.firstChunkPartialDeletionInterval, *chunkDetails.PartiallyDeletedInterval)
						} else if i == numPlans-1 && j == numChunksInGroup-1 && tc.lastChunkPartialDeletionInterval != nil {
							require.Equal(t, *tc.lastChunkPartialDeletionInterval, *chunkDetails.PartiallyDeletedInterval)
						} else {
							require.Equal(t, nilPurgePlanInterval, chunkDetails.PartiallyDeletedInterval)
						}
					}
				}
			}

			require.Equal(t, tc.numChunksToDelete, len(chunkIDs))
		})
	}
}
