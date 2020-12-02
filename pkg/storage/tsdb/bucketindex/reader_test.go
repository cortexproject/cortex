package bucketindex

import (
	"context"
	"path"
	"strings"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/storage/tsdb/testutil"
)

func TestReadIndex_ShouldReturnErrorIfIndexDoesNotExist(t *testing.T) {
	bkt, cleanup := prepareFilesystemBucket(t)
	defer cleanup()

	idx, err := ReadIndex(context.Background(), bkt, "user-1", log.NewNopLogger())
	require.Equal(t, ErrIndexNotFound, err)
	require.Nil(t, idx)
}

func TestReadIndex_ShouldReturnErrorIfIndexIsCorrupted(t *testing.T) {
	const userID = "user-1"

	ctx := context.Background()
	bkt, cleanup := prepareFilesystemBucket(t)
	defer cleanup()

	// Write a corrupted index.
	bkt.Upload(ctx, path.Join(userID, IndexFilename), strings.NewReader("invalid!}"))

	idx, err := ReadIndex(ctx, bkt, userID, log.NewNopLogger())
	require.Equal(t, ErrIndexCorrupted, err)
	require.Nil(t, idx)
}

func TestReadIndex_ShouldReturnTheParsedIndexOnSuccess(t *testing.T) {
	const userID = "user-1"

	ctx := context.Background()
	logger := log.NewNopLogger()

	bkt, cleanup := prepareFilesystemBucket(t)
	defer cleanup()

	// Mock some blocks in the storage.
	bkt = BucketWithMarkersIndex(bkt)
	testutil.MockStorageBlock(t, bkt, userID, 10, 20)
	testutil.MockStorageBlock(t, bkt, userID, 20, 30)
	testutil.MockStorageDeletionMark(t, bkt, userID, testutil.MockStorageBlock(t, bkt, userID, 30, 40))

	// Write the index.
	w := NewWriter(bkt, userID, logger)
	expectedIdx, err := w.WriteIndex(ctx, nil)
	require.NoError(t, err)

	// Read it back and compare.
	actualIdx, err := ReadIndex(ctx, bkt, userID, logger)
	require.NoError(t, err)
	assert.Equal(t, expectedIdx, actualIdx)
}
