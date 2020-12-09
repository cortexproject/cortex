package bucketindex

import (
	"context"
	"strings"
	"testing"

	"github.com/oklog/ulid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGlobalMarkersBucket_Delete_ShouldSucceedIfDeletionMarkDoesNotExistInTheBlockButExistInTheGlobalLocation(t *testing.T) {
	bkt := prepareFilesystemBucket(t)

	ctx := context.Background()
	bkt = BucketWithGlobalMarkers(bkt)

	// Create a mocked block deletion mark in the global location.
	blockID := ulid.MustNew(1, nil)
	globalPath := BlockDeletionMarkFilepath(blockID)
	require.NoError(t, bkt.Upload(ctx, globalPath, strings.NewReader("{}")))

	// Ensure it exists before deleting it.
	ok, err := bkt.Exists(ctx, globalPath)
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, bkt.Delete(ctx, globalPath))

	// Ensure has been actually deleted.
	ok, err = bkt.Exists(ctx, globalPath)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestGlobalMarkersBucket_isBlockDeletionMark(t *testing.T) {
	block1 := ulid.MustNew(1, nil)

	tests := []struct {
		name       string
		expectedOk bool
		expectedID ulid.ULID
	}{
		{
			name:       "",
			expectedOk: false,
		}, {
			name:       "deletion-mark.json",
			expectedOk: false,
		}, {
			name:       block1.String() + "/index",
			expectedOk: false,
		}, {
			name:       block1.String() + "/deletion-mark.json",
			expectedOk: true,
			expectedID: block1,
		}, {
			name:       "/path/to/" + block1.String() + "/deletion-mark.json",
			expectedOk: true,
			expectedID: block1,
		},
	}

	b := BucketWithGlobalMarkers(nil).(*globalMarkersBucket)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actualID, actualOk := b.isBlockDeletionMark(tc.name)
			assert.Equal(t, tc.expectedOk, actualOk)
			assert.Equal(t, tc.expectedID, actualID)
		})
	}
}
