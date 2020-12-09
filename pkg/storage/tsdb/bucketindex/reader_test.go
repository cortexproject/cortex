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
	require.NoError(t, bkt.Upload(ctx, path.Join(userID, IndexCompressedFilename), strings.NewReader("invalid!}")))

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
	bkt = BucketWithGlobalMarkers(bkt)
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

func BenchmarkReadIndex(b *testing.B) {
	const (
		numBlocks             = 1000
		numBlockDeletionMarks = 100
		userID                = "user-1"
	)

	ctx := context.Background()
	logger := log.NewNopLogger()

	bkt, cleanup := prepareFilesystemBucket(b)
	defer cleanup()

	// Mock some blocks and deletion marks in the storage.
	bkt = BucketWithGlobalMarkers(bkt)
	for i := 0; i < numBlocks; i++ {
		minT := int64(i * 10)
		maxT := int64((i + 1) * 10)

		block := testutil.MockStorageBlock(b, bkt, userID, minT, maxT)

		if i < numBlockDeletionMarks {
			testutil.MockStorageDeletionMark(b, bkt, userID, block)
		}
	}

	// Write the index.
	w := NewWriter(bkt, userID, logger)
	_, err := w.WriteIndex(ctx, nil)
	require.NoError(b, err)

	// Read it back once just to make sure the index contains the expected data.
	idx, err := ReadIndex(ctx, bkt, userID, logger)
	require.NoError(b, err)
	require.Len(b, idx.Blocks, numBlocks)
	require.Len(b, idx.BlockDeletionMarks, numBlockDeletionMarks)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		_, err := ReadIndex(ctx, bkt, userID, logger)
		require.NoError(b, err)
	}
}
