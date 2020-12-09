package bucketindex

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/bucket/filesystem"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/testutil"
)

func TestWriter_WriteIndex(t *testing.T) {
	const userID = "user-1"

	bkt, cleanup := prepareFilesystemBucket(t)
	defer cleanup()

	ctx := context.Background()
	logger := log.NewNopLogger()

	// Generate the initial index.
	bkt = BucketWithGlobalMarkers(bkt)
	block1 := testutil.MockStorageBlock(t, bkt, userID, 10, 20)
	block2 := testutil.MockStorageBlock(t, bkt, userID, 20, 30)
	block2Mark := testutil.MockStorageDeletionMark(t, bkt, userID, block2)

	w := NewWriter(bkt, userID, logger)
	returnedIdx, err := w.WriteIndex(ctx, nil)
	require.NoError(t, err)
	writtenIdx, err := ReadIndex(ctx, bkt, userID, logger)
	require.NoError(t, err)

	for _, idx := range []*Index{returnedIdx, writtenIdx} {
		assertBucketIndexEqual(t, idx, bkt, userID,
			[]tsdb.BlockMeta{block1, block2},
			[]*metadata.DeletionMark{block2Mark})
	}

	// Create new blocks, and generate a new index.
	block3 := testutil.MockStorageBlock(t, bkt, userID, 30, 40)
	block4 := testutil.MockStorageBlock(t, bkt, userID, 40, 50)
	block4Mark := testutil.MockStorageDeletionMark(t, bkt, userID, block4)

	returnedIdx, err = w.WriteIndex(ctx, returnedIdx)
	require.NoError(t, err)
	writtenIdx, err = ReadIndex(ctx, bkt, userID, logger)
	require.NoError(t, err)

	for _, idx := range []*Index{returnedIdx, writtenIdx} {
		assertBucketIndexEqual(t, idx, bkt, userID,
			[]tsdb.BlockMeta{block1, block2, block3, block4},
			[]*metadata.DeletionMark{block2Mark, block4Mark})
	}

	// Hard delete a block and generate a new index.
	require.NoError(t, block.Delete(ctx, log.NewNopLogger(), bucket.NewUserBucketClient(userID, bkt), block2.ULID))

	returnedIdx, err = w.WriteIndex(ctx, returnedIdx)
	require.NoError(t, err)
	writtenIdx, err = ReadIndex(ctx, bkt, userID, logger)
	require.NoError(t, err)

	for _, idx := range []*Index{returnedIdx, writtenIdx} {
		assertBucketIndexEqual(t, idx, bkt, userID,
			[]tsdb.BlockMeta{block1, block3, block4},
			[]*metadata.DeletionMark{block4Mark})
	}
}

func TestWriter_GenerateIndex_ShouldSkipPartialBlocks(t *testing.T) {
	const userID = "user-1"

	bkt, cleanup := prepareFilesystemBucket(t)
	defer cleanup()

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

	w := NewWriter(bkt, userID, logger)
	idx, err := w.GenerateIndex(ctx, nil)
	require.NoError(t, err)
	assertBucketIndexEqual(t, idx, bkt, userID,
		[]tsdb.BlockMeta{block1, block2},
		[]*metadata.DeletionMark{block2Mark})
}

func TestWriter_GenerateIndex_ShouldSkipBlocksWithCorruptedMeta(t *testing.T) {
	const userID = "user-1"

	bkt, cleanup := prepareFilesystemBucket(t)
	defer cleanup()

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

	w := NewWriter(bkt, userID, logger)
	idx, err := w.GenerateIndex(ctx, nil)
	require.NoError(t, err)
	assertBucketIndexEqual(t, idx, bkt, userID,
		[]tsdb.BlockMeta{block1, block2},
		[]*metadata.DeletionMark{block2Mark})
}

func TestWriter_GenerateIndex_ShouldSkipCorruptedDeletionMarks(t *testing.T) {
	const userID = "user-1"

	bkt, cleanup := prepareFilesystemBucket(t)
	defer cleanup()

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

	w := NewWriter(bkt, userID, logger)
	idx, err := w.GenerateIndex(ctx, nil)
	require.NoError(t, err)
	assertBucketIndexEqual(t, idx, bkt, userID,
		[]tsdb.BlockMeta{block1, block2, block3},
		[]*metadata.DeletionMark{})
}

func TestWriter_GenerateIndex_NoTenantInTheBucket(t *testing.T) {
	const userID = "user-1"

	ctx := context.Background()
	bkt, cleanup := prepareFilesystemBucket(t)
	defer cleanup()

	for _, oldIdx := range []*Index{nil, {}} {
		w := NewWriter(bkt, userID, log.NewNopLogger())
		idx, err := w.GenerateIndex(ctx, oldIdx)

		require.NoError(t, err)
		assert.Equal(t, IndexVersion1, idx.Version)
		assert.InDelta(t, time.Now().Unix(), idx.UpdatedAt, 2)
		assert.Len(t, idx.Blocks, 0)
		assert.Len(t, idx.BlockDeletionMarks, 0)
	}
}

func prepareFilesystemBucket(t testing.TB) (objstore.Bucket, func()) {
	storageDir, err := ioutil.TempDir(os.TempDir(), "")
	require.NoError(t, err)

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, os.RemoveAll(storageDir))
	}

	return objstore.BucketWithMetrics("test", bkt, nil), cleanup
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
