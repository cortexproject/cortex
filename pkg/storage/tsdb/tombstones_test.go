package tsdb

import (
	"bytes"
	"context"
	"path"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/weaveworks/common/user"
)

func TestTombstones_WritingSameTombstoneTwiceShouldFail(t *testing.T) {

	username := "user"
	requestID := "requestID"

	bkt := objstore.NewInMemBucket()

	ctx := context.Background()
	ctx = user.InjectOrgID(ctx, username)

	tManager := NewTombstoneManager(bkt, username, nil, log.NewNopLogger())

	//create the tombstone
	tombstone := NewTombstone(username, 0, 0, 0, 1, []string{"match"}, requestID, StatePending)
	err := tManager.WriteTombstone(ctx, tombstone)
	require.NoError(t, err)

	filename := requestID + "." + string(StatePending) + ".json"
	exists, _ := bkt.Exists(ctx, path.Join(username, TombstonePath, filename))
	require.True(t, exists)

	// Creating the same tombstone twice should result in an error
	err = tManager.WriteTombstone(ctx, tombstone)
	require.ErrorIs(t, err, ErrTombstoneAlreadyExists)

}

func TestTombstonesExists(t *testing.T) {
	const username = "user"
	const requestID = "requestID"

	for name, tc := range map[string]struct {
		objects            map[string][]byte
		targetRequestState BlockDeleteRequestState
		exists             bool
	}{
		"no tombstones exist": {
			objects:            nil,
			targetRequestState: StatePending,
			exists:             false,
		},

		"tombstone exists but different state": {
			objects: map[string][]byte{
				username + "/tombstones/" + requestID + "." + string(StateProcessed) + ".json": []byte("data"),
				username + "/tombstones/" + requestID + "." + string(StateCancelled) + ".json": []byte("data"),
			},
			targetRequestState: StatePending,
			exists:             false,
		},

		"tombstone exists with correct state": {
			objects: map[string][]byte{
				username + "/tombstones/" + requestID + "." + string(StatePending) + ".json": []byte("data"),
			},
			targetRequestState: StatePending,
			exists:             true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			ctx = user.InjectOrgID(ctx, username)

			bkt := objstore.NewInMemBucket()
			// "upload" sample tombstone files
			for objName, data := range tc.objects {
				require.NoError(t, bkt.Upload(context.Background(), objName, bytes.NewReader(data)))
			}
			tManager := NewTombstoneManager(bkt, username, nil, log.NewNopLogger())

			res, err := tManager.TombstoneExists(ctx, requestID, tc.targetRequestState)
			require.NoError(t, err)
			require.Equal(t, tc.exists, res)
		})
	}
}

func TestTombstonesDeletion(t *testing.T) {
	const username = "user"
	const requestID = "requestID"

	ctx := context.Background()
	ctx = user.InjectOrgID(ctx, username)

	tPending := NewTombstone(username, 0, 0, 0, 0, []string{}, requestID, StatePending)

	tPendingPath := username + "/tombstones/" + requestID + "." + string(StatePending) + ".json"
	tProcessedPath := username + "/tombstones/" + requestID + "." + string(StateProcessed) + ".json"

	bkt := objstore.NewInMemBucket()
	// "upload" sample tombstone file
	require.NoError(t, bkt.Upload(context.Background(), tPendingPath, bytes.NewReader([]byte("data"))))
	require.NoError(t, bkt.Upload(context.Background(), tProcessedPath, bytes.NewReader([]byte("data"))))

	tManager := NewTombstoneManager(bkt, tPending.UserID, nil, log.NewNopLogger())
	require.NoError(t, tManager.DeleteTombstoneFile(ctx, tPending.RequestID, tPending.State))

	// make sure the pending tombstone was deleted
	exists, _ := bkt.Exists(ctx, tPendingPath)
	require.False(t, exists)

	// the processed tombstone with the same requestID should still be in the bucket
	exists, _ = bkt.Exists(ctx, tProcessedPath)
	require.True(t, exists)

}

func TestTombstoneUpdateState(t *testing.T) {
	const username = "user"
	const requestID = "requestID"

	ctx := context.Background()
	ctx = user.InjectOrgID(ctx, username)

	tPending := NewTombstone(username, 0, 0, 0, 0, []string{}, requestID, StatePending)
	tPendingPath := username + "/tombstones/" + requestID + "." + string(StatePending) + ".json"

	bkt := objstore.NewInMemBucket()
	tManager := NewTombstoneManager(bkt, username, nil, log.NewNopLogger())

	// "upload" sample tombstone file
	require.NoError(t, bkt.Upload(context.Background(), tPendingPath, bytes.NewReader([]byte("data"))))

	tProcessed, err := tManager.UpdateTombstoneState(ctx, tPending, StateProcessed)
	require.NoError(t, err)

	// make sure the pending tombstone was deleted
	exists, _ := bkt.Exists(ctx, tPendingPath)
	require.False(t, exists)

	// check that the new tombstone with the updated state has been created
	tProcessedPath := username + "/tombstones/" + tProcessed.RequestID + "." + string(tProcessed.State) + ".json"
	exists, _ = bkt.Exists(ctx, tProcessedPath)
	require.True(t, exists)
}

func TestGetSingleTombstone(t *testing.T) {
	const username = "user"
	const requestID = "requestID"

	ctx := context.Background()
	ctx = user.InjectOrgID(ctx, username)

	// When getting a specific request id, there could be a case where multiple
	// files exist for the same request but with different extensions to indicate a
	// different state. If thats the case, then the older state should be deleted.

	// Add multiple tombstones with the same request id but different states
	tPending := NewTombstone(username, 0, 0, 0, 0, []string{"node_exporter"}, requestID, StatePending)
	tProcessed := NewTombstone(username, 10, 20, 30, 60, []string{"node_exporter"}, requestID, StateProcessed)

	bkt := objstore.NewInMemBucket()
	tManager := NewTombstoneManager(bkt, tPending.UserID, nil, log.NewNopLogger())

	// first add the tombstone files to the object store
	require.NoError(t, tManager.WriteTombstone(ctx, tPending))
	require.NoError(t, tManager.WriteTombstone(ctx, tProcessed))

	tRetrieved, err := tManager.GetTombstoneByIDForUser(ctx, requestID)
	require.NoError(t, err)

	//verify that all the information was read correctly
	require.Equal(t, tProcessed.StartTime, tRetrieved.StartTime)
	require.Equal(t, tProcessed.EndTime, tRetrieved.EndTime)
	require.Equal(t, tProcessed.RequestCreatedAt, tRetrieved.RequestCreatedAt)
	require.Equal(t, tProcessed.StateCreatedAt, tRetrieved.StateCreatedAt)
	require.Equal(t, tProcessed.Selectors, tRetrieved.Selectors)
	require.Equal(t, tProcessed.RequestID, tRetrieved.RequestID)
	require.Equal(t, tProcessed.UserID, tRetrieved.UserID)
	require.Equal(t, tProcessed.State, tRetrieved.State)

	// Get single tombstone that doesn't exist should return nil
	tRetrieved, err = tManager.GetTombstoneByIDForUser(ctx, "unknownRequestID")
	require.NoError(t, err)
	require.Nil(t, tRetrieved)
}

func TestGetAllTombstones(t *testing.T) {
	const username = "user"
	ctx := context.Background()
	ctx = user.InjectOrgID(ctx, username)
	bkt := objstore.NewInMemBucket()

	tManager := NewTombstoneManager(bkt, username, nil, log.NewNopLogger())

	tombstonesInput := []*Tombstone{
		NewTombstone(username, 0, 0, 0, 0, []string{}, "request1", StatePending),
		NewTombstone(username, 0, 0, 0, 0, []string{}, "request1", StateCancelled),
		NewTombstone(username, 0, 0, 0, 0, []string{}, "request2", StatePending),
		NewTombstone(username, 0, 0, 0, 0, []string{}, "request3", StatePending),
		NewTombstone(username, 0, 0, 0, 0, []string{}, "request4", StateProcessed),
		NewTombstone(username, 0, 0, 0, 0, []string{}, "request5", StateCancelled),
		NewTombstone(username, 0, 0, 0, 0, []string{}, "request6", StatePending),
		NewTombstone(username, 0, 0, 0, 0, []string{}, "request6", StateProcessed),
	}

	requiredOutput := map[string]BlockDeleteRequestState{
		"request1": StateCancelled,
		"request2": StatePending,
		"request3": StatePending,
		"request4": StateProcessed,
		"request5": StateCancelled,
		"request6": StateProcessed,
	}

	// add all tombstones to the bkt
	for _, ts := range tombstonesInput {
		require.NoError(t, tManager.WriteTombstone(ctx, ts))
	}

	tombstonesOutput, err := tManager.GetAllTombstonesForUser(ctx)
	require.NoError(t, err)

	outputMap := make(map[string]BlockDeleteRequestState)
	for _, ts := range tombstonesOutput {
		_, exists := outputMap[ts.RequestID]
		// There should not be more than one ts for each request id
		require.False(t, exists)

		outputMap[ts.RequestID] = ts.State
	}

	require.Equal(t, requiredOutput, outputMap)
}

func TestTombstoneReadWithInvalidFileName(t *testing.T) {
	const username = "user"
	const requestID = "requestID"
	bkt := objstore.NewInMemBucket()

	ctx := context.Background()
	ctx = user.InjectOrgID(ctx, username)

	tManager := NewTombstoneManager(bkt, username, nil, log.NewNopLogger())

	{
		tInvalidPath := username + "/tombstones/" + requestID + "." + string(StatePending)
		_, err := tManager.ReadTombstone(ctx, tInvalidPath)

		require.ErrorIs(t, err, ErrInvalidDeletionRequestState)
	}

	{
		tInvalidPath := username + "/tombstones/" + requestID
		_, err := tManager.ReadTombstone(ctx, tInvalidPath)

		require.ErrorIs(t, err, ErrInvalidDeletionRequestState)
	}

	{
		tInvalidPath := username + "/tombstones/" + requestID + ".json." + string(StatePending)
		_, err := tManager.ReadTombstone(ctx, tInvalidPath)
		require.ErrorIs(t, err, ErrInvalidDeletionRequestState)
	}

}
