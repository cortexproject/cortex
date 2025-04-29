package bucketindex

import (
	"bytes"
	"context"
	"encoding/json"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/parquet"
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
	returnedIdx, _, _, err := w.UpdateIndex(ctx, nil)
	require.NoError(t, err)
	assertBucketIndexEqual(t, returnedIdx, bkt, userID,
		[]tsdb.BlockMeta{block1, block2},
		[]*metadata.DeletionMark{block2Mark})

	// Create new blocks, and update the index.
	block3 := testutil.MockStorageBlock(t, bkt, userID, 30, 40)
	block4 := testutil.MockStorageBlock(t, bkt, userID, 40, 50)
	block4Mark := testutil.MockStorageDeletionMark(t, bkt, userID, block4)

	returnedIdx, _, _, err = w.UpdateIndex(ctx, returnedIdx)
	require.NoError(t, err)
	assertBucketIndexEqual(t, returnedIdx, bkt, userID,
		[]tsdb.BlockMeta{block1, block2, block3, block4},
		[]*metadata.DeletionMark{block2Mark, block4Mark})

	// Hard delete a block and update the index.
	require.NoError(t, block.Delete(ctx, log.NewNopLogger(), bucket.NewUserBucketClient(userID, bkt, nil), block2.ULID))

	returnedIdx, _, _, err = w.UpdateIndex(ctx, returnedIdx)
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
	idx, partials, _, err := w.UpdateIndex(ctx, nil)
	require.NoError(t, err)
	assertBucketIndexEqual(t, idx, bkt, userID,
		[]tsdb.BlockMeta{block1, block2},
		[]*metadata.DeletionMark{block2Mark})

	assert.Len(t, partials, 1)
	assert.True(t, errors.Is(partials[block3.ULID], ErrBlockMetaNotFound))
}

func TestUpdater_UpdateIndex_ShouldNotIncreaseOperationFailureMetric(t *testing.T) {
	const userID = "user-1"

	bkt, _ := testutil.PrepareFilesystemBucket(t)

	ctx := context.Background()
	logger := log.NewNopLogger()
	registry := prometheus.NewRegistry()

	// Mock some blocks in the storage.
	bkt = BucketWithGlobalMarkers(bkt)
	bkt = objstore.WrapWithMetrics(bkt, prometheus.WrapRegistererWithPrefix("thanos_", registry), "test-bucket")
	block1 := testutil.MockStorageBlock(t, bkt, userID, 10, 20)
	block2 := testutil.MockStorageBlock(t, bkt, userID, 20, 30)
	block3 := testutil.MockStorageBlock(t, bkt, userID, 30, 40)
	block2Mark := testutil.MockStorageDeletionMark(t, bkt, userID, block2)

	// Delete a block's meta.json to simulate a partial block.
	require.NoError(t, bkt.Delete(ctx, path.Join(userID, block3.ULID.String(), metadata.MetaFilename)))

	w := NewUpdater(bkt, userID, nil, logger)
	idx, partials, _, err := w.UpdateIndex(ctx, nil)
	require.NoError(t, err)
	assertBucketIndexEqual(t, idx, bkt, userID,
		[]tsdb.BlockMeta{block1, block2},
		[]*metadata.DeletionMark{block2Mark})

	assert.Len(t, partials, 1)
	assert.True(t, errors.Is(partials[block3.ULID], ErrBlockMetaNotFound))

	assert.NoError(t, prom_testutil.GatherAndCompare(registry, strings.NewReader(`
			# HELP thanos_objstore_bucket_operation_failures_total Total number of operations against a bucket that failed, but were not expected to fail in certain way from caller perspective. Those errors have to be investigated.
			# TYPE thanos_objstore_bucket_operation_failures_total counter
			thanos_objstore_bucket_operation_failures_total{bucket="test-bucket",operation="attributes"} 0
            thanos_objstore_bucket_operation_failures_total{bucket="test-bucket",operation="delete"} 0
            thanos_objstore_bucket_operation_failures_total{bucket="test-bucket",operation="exists"} 0
            thanos_objstore_bucket_operation_failures_total{bucket="test-bucket",operation="get"} 0
            thanos_objstore_bucket_operation_failures_total{bucket="test-bucket",operation="get_range"} 0
            thanos_objstore_bucket_operation_failures_total{bucket="test-bucket",operation="iter"} 0
            thanos_objstore_bucket_operation_failures_total{bucket="test-bucket",operation="upload"} 0
		`), "thanos_objstore_bucket_operation_failures_total"))
}

func TestUpdater_UpdateIndex_ShouldNotIncreaseOperationFailureMetricCustomerKey(t *testing.T) {
	const userID = "user-1"

	bkt, _ := testutil.PrepareFilesystemBucket(t)

	ctx := context.Background()
	logger := log.NewNopLogger()
	registry := prometheus.NewRegistry()

	// Mock some blocks in the storage.
	bkt = BucketWithGlobalMarkers(bkt)
	bkt = objstore.WrapWithMetrics(bkt, prometheus.WrapRegistererWithPrefix("thanos_", registry), "test-bucket")
	block1 := testutil.MockStorageBlock(t, bkt, userID, 10, 20)
	block2 := testutil.MockStorageBlock(t, bkt, userID, 20, 30)

	bkt = &testutil.MockBucketFailure{
		Bucket: bkt,
		GetFailures: map[string]error{
			path.Join(userID, block2.ULID.String(), "meta.json"): testutil.ErrKeyAccessDeniedError,
		},
	}

	w := NewUpdater(bkt, userID, nil, logger)
	idx, partials, _, err := w.UpdateIndex(ctx, nil)
	require.NoError(t, err)
	assert.Len(t, partials, 1)
	assert.True(t, errors.Is(partials[block2.ULID], errBlockMetaKeyAccessDeniedErr))
	assertBucketIndexEqual(t, idx, bkt, userID,
		[]tsdb.BlockMeta{block1},
		[]*metadata.DeletionMark{})

	assert.NoError(t, prom_testutil.GatherAndCompare(registry, strings.NewReader(`
			# HELP thanos_objstore_bucket_operation_failures_total Total number of operations against a bucket that failed, but were not expected to fail in certain way from caller perspective. Those errors have to be investigated.
			# TYPE thanos_objstore_bucket_operation_failures_total counter
			thanos_objstore_bucket_operation_failures_total{bucket="test-bucket",operation="attributes"} 0
            thanos_objstore_bucket_operation_failures_total{bucket="test-bucket",operation="delete"} 0
            thanos_objstore_bucket_operation_failures_total{bucket="test-bucket",operation="exists"} 0
            thanos_objstore_bucket_operation_failures_total{bucket="test-bucket",operation="get"} 0
            thanos_objstore_bucket_operation_failures_total{bucket="test-bucket",operation="get_range"} 0
            thanos_objstore_bucket_operation_failures_total{bucket="test-bucket",operation="iter"} 0
            thanos_objstore_bucket_operation_failures_total{bucket="test-bucket",operation="upload"} 0
		`), "thanos_objstore_bucket_operation_failures_total"))
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
	block4 := testutil.MockStorageBlock(t, bkt, userID, 50, 50)
	block2Mark := testutil.MockStorageDeletionMark(t, bkt, userID, block2)
	testutil.MockStorageNonCompactionMark(t, bkt, userID, block4)

	// Overwrite a block's meta.json with invalid data.
	require.NoError(t, bkt.Upload(ctx, path.Join(userID, block3.ULID.String(), metadata.MetaFilename), bytes.NewReader([]byte("invalid!}"))))

	w := NewUpdater(bkt, userID, nil, logger)
	idx, partials, nonCompactBlocks, err := w.UpdateIndex(ctx, nil)
	require.NoError(t, err)
	assertBucketIndexEqual(t, idx, bkt, userID,
		[]tsdb.BlockMeta{block1, block2, block4},
		[]*metadata.DeletionMark{block2Mark})

	assert.Len(t, partials, 1)
	assert.True(t, errors.Is(partials[block3.ULID], ErrBlockMetaCorrupted))
	assert.Equal(t, nonCompactBlocks, int64(1))
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
	block4 := testutil.MockStorageBlock(t, bkt, userID, 40, 50)
	block2Mark := testutil.MockStorageDeletionMark(t, bkt, userID, block2)
	block4Mark := testutil.MockStorageNonCompactionMark(t, bkt, userID, block4)

	// Overwrite a block's deletion-mark.json with invalid data.
	require.NoError(t, bkt.Upload(ctx, path.Join(userID, block2Mark.ID.String(), metadata.DeletionMarkFilename), bytes.NewReader([]byte("invalid!}"))))
	require.NoError(t, bkt.Upload(ctx, path.Join(userID, block4Mark.ID.String(), metadata.NoCompactMarkFilename), bytes.NewReader([]byte("invalid!}"))))

	w := NewUpdater(bkt, userID, nil, logger)
	idx, partials, nonCompactBlocks, err := w.UpdateIndex(ctx, nil)
	require.NoError(t, err)
	assertBucketIndexEqual(t, idx, bkt, userID,
		[]tsdb.BlockMeta{block1, block2, block3, block4},
		[]*metadata.DeletionMark{})
	assert.Empty(t, partials)
	assert.Equal(t, nonCompactBlocks, int64(1))
}

func TestUpdater_UpdateIndex_ShouldSkipBlockMarkedForDeletionWithMissingGlobalMarker(t *testing.T) {
	const userID = "user-1"

	bkt, _ := testutil.PrepareFilesystemBucket(t)

	ctx := context.Background()
	logger := log.NewNopLogger()

	// Mock some blocks in the storage.
	bkt = BucketWithGlobalMarkers(bkt)
	block1 := testutil.MockStorageBlock(t, bkt, userID, 10, 20)
	block2 := testutil.MockStorageBlock(t, bkt, userID, 20, 30)

	oldIdx := &Index{
		Version: IndexVersion1,
		Blocks: Blocks{&Block{
			ID:         block2.ULID,
			MinTime:    block2.MinTime,
			MaxTime:    block2.MaxTime,
			UploadedAt: getBlockUploadedAt(t, bkt, userID, block2.ULID),
		}},
		BlockDeletionMarks: BlockDeletionMarks{&BlockDeletionMark{
			ID:           block2.ULID,
			DeletionTime: time.Now().Add(-time.Minute).Unix(),
		}},
	}

	w := NewUpdater(bkt, userID, nil, logger)
	idx, partials, nonCompactBlocks, err := w.UpdateIndex(ctx, oldIdx)
	require.NoError(t, err)
	assertBucketIndexEqual(t, idx, bkt, userID,
		[]tsdb.BlockMeta{block1},
		[]*metadata.DeletionMark{})
	assert.Empty(t, partials)
	assert.Empty(t, nonCompactBlocks)
}

func TestUpdater_UpdateIndex_NoTenantInTheBucket(t *testing.T) {
	const userID = "user-1"

	ctx := context.Background()
	bkt, _ := testutil.PrepareFilesystemBucket(t)

	for _, oldIdx := range []*Index{nil, {}} {
		w := NewUpdater(bkt, userID, nil, log.NewNopLogger())
		idx, partials, _, err := w.UpdateIndex(ctx, oldIdx)

		require.NoError(t, err)
		assert.Equal(t, IndexVersion1, idx.Version)
		assert.InDelta(t, time.Now().Unix(), idx.UpdatedAt, 2)
		assert.Len(t, idx.Blocks, 0)
		assert.Len(t, idx.BlockDeletionMarks, 0)
		assert.Empty(t, partials)
	}
}

func TestUpdater_UpdateIndex_WithParquet(t *testing.T) {
	const userID = "user-1"

	bkt, _ := testutil.PrepareFilesystemBucket(t)

	ctx := context.Background()
	logger := log.NewNopLogger()

	// Generate the initial index.
	bkt = BucketWithGlobalMarkers(bkt)
	block1 := testutil.MockStorageBlock(t, bkt, userID, 10, 20)
	block2 := testutil.MockStorageBlock(t, bkt, userID, 20, 30)
	block2Mark := testutil.MockStorageDeletionMark(t, bkt, userID, block2)
	// Add parquet marker to block 1.
	block1ParquetMark := testutil.MockStorageParquetConverterMark(t, bkt, userID, block1)

	w := NewUpdater(bkt, userID, nil, logger).EnableParquet()
	returnedIdx, _, _, err := w.UpdateIndex(ctx, nil)
	require.NoError(t, err)
	assertBucketIndexEqualWithParquet(t, returnedIdx, bkt, userID,
		[]tsdb.BlockMeta{block1, block2},
		[]*metadata.DeletionMark{block2Mark}, map[string]*parquet.ConverterMarkMeta{
			block1.ULID.String(): {Version: block1ParquetMark.Version},
		})

	// Create new blocks, and update the index.
	block3 := testutil.MockStorageBlock(t, bkt, userID, 30, 40)
	block4 := testutil.MockStorageBlock(t, bkt, userID, 40, 50)
	block4Mark := testutil.MockStorageDeletionMark(t, bkt, userID, block4)

	returnedIdx, _, _, err = w.UpdateIndex(ctx, returnedIdx)
	require.NoError(t, err)
	assertBucketIndexEqualWithParquet(t, returnedIdx, bkt, userID,
		[]tsdb.BlockMeta{block1, block2, block3, block4},
		[]*metadata.DeletionMark{block2Mark, block4Mark},
		map[string]*parquet.ConverterMarkMeta{
			block1.ULID.String(): {Version: block1ParquetMark.Version},
		})

	// Hard delete a block and update the index.
	require.NoError(t, block.Delete(ctx, log.NewNopLogger(), bucket.NewUserBucketClient(userID, bkt, nil), block2.ULID))

	returnedIdx, _, _, err = w.UpdateIndex(ctx, returnedIdx)
	require.NoError(t, err)
	assertBucketIndexEqualWithParquet(t, returnedIdx, bkt, userID,
		[]tsdb.BlockMeta{block1, block3, block4},
		[]*metadata.DeletionMark{block4Mark}, map[string]*parquet.ConverterMarkMeta{
			block1.ULID.String(): {Version: block1ParquetMark.Version},
		})

	// Upload parquet marker to an old block and update index
	block3ParquetMark := testutil.MockStorageParquetConverterMark(t, bkt, userID, block3)
	returnedIdx, _, _, err = w.UpdateIndex(ctx, returnedIdx)
	require.NoError(t, err)
	assertBucketIndexEqualWithParquet(t, returnedIdx, bkt, userID,
		[]tsdb.BlockMeta{block1, block3, block4},
		[]*metadata.DeletionMark{block4Mark}, map[string]*parquet.ConverterMarkMeta{
			block1.ULID.String(): {Version: block1ParquetMark.Version},
			block3.ULID.String(): {Version: block3ParquetMark.Version},
		})
}

func TestUpdater_UpdateParquetBlockIndexEntry(t *testing.T) {
	const userID = "user-1"
	ctx := context.Background()
	logger := log.NewNopLogger()

	tests := []struct {
		name              string
		setupBucket       func(t *testing.T, bkt objstore.InstrumentedBucket, blockID ulid.ULID) objstore.InstrumentedBucket
		expectedError     error
		expectParquet     bool
		expectParquetMeta *parquet.ConverterMarkMeta
	}{
		{
			name: "should successfully read parquet marker",
			setupBucket: func(t *testing.T, bkt objstore.InstrumentedBucket, blockID ulid.ULID) objstore.InstrumentedBucket {
				parquetMark := parquet.ConverterMarkMeta{
					Version: 1,
				}
				data, err := json.Marshal(parquetMark)
				require.NoError(t, err)
				require.NoError(t, bkt.Upload(ctx, path.Join(userID, blockID.String(), parquet.ConverterMarkerFileName), bytes.NewReader(data)))
				return bkt
			},
			expectedError:     nil,
			expectParquet:     true,
			expectParquetMeta: &parquet.ConverterMarkMeta{Version: 1},
		},
		{
			name: "should handle missing parquet marker",
			setupBucket: func(t *testing.T, bkt objstore.InstrumentedBucket, blockID ulid.ULID) objstore.InstrumentedBucket {
				// Don't upload any parquet marker
				return bkt
			},
			expectedError: nil,
			expectParquet: false,
		},
		{
			name: "should handle access denied",
			setupBucket: func(t *testing.T, bkt objstore.InstrumentedBucket, blockID ulid.ULID) objstore.InstrumentedBucket {
				return &testutil.MockBucketFailure{
					Bucket: bkt,
					GetFailures: map[string]error{
						path.Join(userID, blockID.String(), parquet.ConverterMarkerFileName): testutil.ErrKeyAccessDeniedError,
					},
				}
			},
			expectedError: nil,
			expectParquet: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bkt, _ := testutil.PrepareFilesystemBucket(t)
			blockID := ulid.MustNew(1, nil)
			block := &Block{ID: blockID}

			// Setup the bucket with test data
			bkt = tc.setupBucket(t, bkt, blockID)

			// Create an instrumented bucket wrapper
			registry := prometheus.NewRegistry()
			ibkt := objstore.WrapWithMetrics(bkt, prometheus.WrapRegistererWithPrefix("thanos_", registry), "test-bucket")
			w := NewUpdater(ibkt, userID, nil, logger)

			err := w.updateParquetBlockIndexEntry(ctx, blockID, block)
			if tc.expectedError != nil {
				assert.True(t, errors.Is(err, tc.expectedError))
			} else {
				assert.NoError(t, err)
			}

			if tc.expectParquet {
				assert.NotNil(t, block.Parquet)
				assert.Equal(t, tc.expectParquetMeta, block.Parquet)
			} else {
				assert.Nil(t, block.Parquet)
			}
		})
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

func assertBucketIndexEqualWithParquet(t testing.TB, idx *Index, bkt objstore.Bucket, userID string, expectedBlocks []tsdb.BlockMeta, expectedDeletionMarks []*metadata.DeletionMark, parquetBlocks map[string]*parquet.ConverterMarkMeta) {
	assert.Equal(t, IndexVersion1, idx.Version)
	assert.InDelta(t, time.Now().Unix(), idx.UpdatedAt, 2)

	// Build the list of expected block index entries.
	var expectedBlockEntries []*Block
	for _, b := range expectedBlocks {
		block := &Block{
			ID:         b.ULID,
			MinTime:    b.MinTime,
			MaxTime:    b.MaxTime,
			UploadedAt: getBlockUploadedAt(t, bkt, userID, b.ULID),
		}
		if meta, ok := parquetBlocks[b.ULID.String()]; ok {
			block.Parquet = meta
		}
		expectedBlockEntries = append(expectedBlockEntries, block)
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
