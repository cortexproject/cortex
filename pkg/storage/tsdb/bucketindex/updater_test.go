package bucketindex

import (
	"bytes"
	"context"
	"path"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/testutil"
)

func TestUpdater_UpdateIndex(t *testing.T) {
	const userID = "user-1"

	bkt, _ := testutil.PrepareFilesystemBucket(t)

	ctx := context.Background()
	logger := log.NewNopLogger()

	// Generate the initial index.
	bkt = BucketWithGlobalMarkers(bkt)
	block1 := testutil.MockStorageBlock(t, bkt, userID, 10, 20)
	block2 := testutil.MockStorageBlock(t, bkt, userID, 20, 30)
	block2Mark := testutil.MockStorageDeletionMark(t, bkt, userID, block2)

	w := NewUpdater(bkt, userID, nil, logger)
	returnedIdx, _, err := w.UpdateIndex(ctx, nil)
	require.NoError(t, err)
	assertBucketIndexEqual(t, returnedIdx, bkt, userID,
		[]tsdb.BlockMeta{block1, block2},
		[]*metadata.DeletionMark{block2Mark})

	// Create new blocks, and update the index.
	block3 := testutil.MockStorageBlock(t, bkt, userID, 30, 40)
	block4 := testutil.MockStorageBlock(t, bkt, userID, 40, 50)
	block4Mark := testutil.MockStorageDeletionMark(t, bkt, userID, block4)

	returnedIdx, _, err = w.UpdateIndex(ctx, returnedIdx)
	require.NoError(t, err)
	assertBucketIndexEqual(t, returnedIdx, bkt, userID,
		[]tsdb.BlockMeta{block1, block2, block3, block4},
		[]*metadata.DeletionMark{block2Mark, block4Mark})

	// Hard delete a block and update the index.
	require.NoError(t, block.Delete(ctx, log.NewNopLogger(), bucket.NewUserBucketClient(userID, bkt, nil), block2.ULID))

	returnedIdx, _, err = w.UpdateIndex(ctx, returnedIdx)
	require.NoError(t, err)
	assertBucketIndexEqual(t, returnedIdx, bkt, userID,
		[]tsdb.BlockMeta{block1, block3, block4},
		[]*metadata.DeletionMark{block4Mark})
}

func TestUpdater_UpdateIndex_ShouldSkipPartialBlocks(t *testing.T) {
	const userID = "user-1"

	bkt, _ := testutil.PrepareFilesystemBucket(t)

	ctx := context.Background()
	logger := log.NewNopLogger()

	// Mock some blocks in the storage.
	bkt = BucketWithGlobalMarkers(bkt)
	block1 := testutil.MockStorageBlock(t, bkt, userID, 10, 20)
	block2 := testutil.MockStorageBlock(t, bkt, userID, 20, 30)
	block3 := testutil.MockStorageBlock(t, bkt, userID, 30, 40)
	block2Mark := testutil.MockStorageDeletionMark(t, bkt, userID, block2)

	// Delete a block's meta.json to simulate a partial block.
	require.NoError(t, bkt.Delete(ctx, path.Join(userID, block3.ULID.String(), metadata.MetaFilename)))

	w := NewUpdater(bkt, userID, nil, logger)
	idx, partials, err := w.UpdateIndex(ctx, nil)
	require.NoError(t, err)
	assertBucketIndexEqual(t, idx, bkt, userID,
		[]tsdb.BlockMeta{block1, block2},
		[]*metadata.DeletionMark{block2Mark})

	assert.Len(t, partials, 1)
	assert.True(t, errors.Is(partials[block3.ULID], ErrBlockMetaNotFound))
}

func TestUpdater_UpdateIndex_ShouldSkipBlocksWithCorruptedMeta(t *testing.T) {
	const userID = "user-1"

	bkt, _ := testutil.PrepareFilesystemBucket(t)

	ctx := context.Background()
	logger := log.NewNopLogger()

	// Mock some blocks in the storage.
	bkt = BucketWithGlobalMarkers(bkt)
	block1 := testutil.MockStorageBlock(t, bkt, userID, 10, 20)
	block2 := testutil.MockStorageBlock(t, bkt, userID, 20, 30)
	block3 := testutil.MockStorageBlock(t, bkt, userID, 30, 40)
	block2Mark := testutil.MockStorageDeletionMark(t, bkt, userID, block2)

	// Overwrite a block's meta.json with invalid data.
	require.NoError(t, bkt.Upload(ctx, path.Join(userID, block3.ULID.String(), metadata.MetaFilename), bytes.NewReader([]byte("invalid!}"))))

	w := NewUpdater(bkt, userID, nil, logger)
	idx, partials, err := w.UpdateIndex(ctx, nil)
	require.NoError(t, err)
	assertBucketIndexEqual(t, idx, bkt, userID,
		[]tsdb.BlockMeta{block1, block2},
		[]*metadata.DeletionMark{block2Mark})

	assert.Len(t, partials, 1)
	assert.True(t, errors.Is(partials[block3.ULID], ErrBlockMetaCorrupted))
}

func TestUpdater_UpdateIndex_ShouldSkipCorruptedDeletionMarks(t *testing.T) {
	const userID = "user-1"

	bkt, _ := testutil.PrepareFilesystemBucket(t)

	ctx := context.Background()
	logger := log.NewNopLogger()

	// Mock some blocks in the storage.
	bkt = BucketWithGlobalMarkers(bkt)
	block1 := testutil.MockStorageBlock(t, bkt, userID, 10, 20)
	block2 := testutil.MockStorageBlock(t, bkt, userID, 20, 30)
	block3 := testutil.MockStorageBlock(t, bkt, userID, 30, 40)
	block2Mark := testutil.MockStorageDeletionMark(t, bkt, userID, block2)

	// Overwrite a block's deletion-mark.json with invalid data.
	require.NoError(t, bkt.Upload(ctx, path.Join(userID, block2Mark.ID.String(), metadata.DeletionMarkFilename), bytes.NewReader([]byte("invalid!}"))))

	w := NewUpdater(bkt, userID, nil, logger)
	idx, partials, err := w.UpdateIndex(ctx, nil)
	require.NoError(t, err)
	assertBucketIndexEqual(t, idx, bkt, userID,
		[]tsdb.BlockMeta{block1, block2, block3},
		[]*metadata.DeletionMark{})
	assert.Empty(t, partials)
}

func TestUpdater_UpdateIndex_NoTenantInTheBucket(t *testing.T) {
	const userID = "user-1"

	ctx := context.Background()
	bkt, _ := testutil.PrepareFilesystemBucket(t)

	for _, oldIdx := range []*Index{nil, {}} {
		w := NewUpdater(bkt, userID, nil, log.NewNopLogger())
		idx, partials, err := w.UpdateIndex(ctx, oldIdx)

		require.NoError(t, err)
		assert.Equal(t, IndexVersion1, idx.Version)
		assert.InDelta(t, time.Now().Unix(), idx.UpdatedAt, 2)
		assert.Len(t, idx.Blocks, 0)
		assert.Len(t, idx.BlockDeletionMarks, 0)
		assert.Empty(t, partials)
	}
}

func getBlockUploadedAt(t testing.TB, bkt objstore.Bucket, userID string, blockID ulid.ULID) int64 {
	metaFile := path.Join(userID, blockID.String(), block.MetaFilename)

	attrs, err := bkt.Attributes(context.Background(), metaFile)
	require.NoError(t, err)

	return attrs.LastModified.Unix()
}

func assertBucketIndexEqual(t testing.TB, idx *Index, bkt objstore.Bucket, userID string, expectedBlocks []tsdb.BlockMeta, expectedDeletionMarks []*metadata.DeletionMark) {
	assert.Equal(t, IndexVersion1, idx.Version)
	assert.InDelta(t, time.Now().Unix(), idx.UpdatedAt, 2)

	// Build the list of expected block index entries.
	var expectedBlockEntries []*Block
	for _, b := range expectedBlocks {
		expectedBlockEntries = append(expectedBlockEntries, &Block{
			ID:         b.ULID,
			MinTime:    b.MinTime,
			MaxTime:    b.MaxTime,
			UploadedAt: getBlockUploadedAt(t, bkt, userID, b.ULID),
		})
	}

	assert.ElementsMatch(t, expectedBlockEntries, idx.Blocks)

	// Build the list of expected block deletion mark index entries.
	var expectedMarkEntries []*BlockDeletionMark
	for _, m := range expectedDeletionMarks {
		expectedMarkEntries = append(expectedMarkEntries, &BlockDeletionMark{
			ID:           m.ID,
			DeletionTime: m.DeletionTime,
		})
	}

	assert.ElementsMatch(t, expectedMarkEntries, idx.BlockDeletionMarks)
}
