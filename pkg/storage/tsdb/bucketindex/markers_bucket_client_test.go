package bucketindex

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/pkg/storage/bucket/s3"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_testutil "github.com/cortexproject/cortex/pkg/storage/tsdb/testutil"
)

func TestGlobalMarker_ShouldUploadGlobalLocation(t *testing.T) {
	block1 := ulid.MustNew(1, nil)

	tests := []struct {
		mark       string
		globalpath string
	}{
		{
			mark:       metadata.DeletionMarkFilename,
			globalpath: "markers/" + block1.String() + "-deletion-mark.json",
		},
		{
			mark:       metadata.NoCompactMarkFilename,
			globalpath: "markers/" + block1.String() + "-no-compact-mark.json",
		},
	}

	for _, tc := range tests {
		t.Run(tc.mark, func(t *testing.T) {
			originalPath := block1.String() + "/" + tc.mark
			bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)

			ctx := context.Background()
			bkt = BucketWithGlobalMarkers(bkt)

			err := bkt.Upload(ctx, originalPath, strings.NewReader("{}"))
			require.NoError(t, err)

			// Ensure it exists on originalPath
			ok, err := bkt.Exists(ctx, originalPath)
			require.NoError(t, err)
			require.True(t, ok)

			// Ensure it exists on globalPath
			ok, err = bkt.Exists(ctx, tc.globalpath)
			require.NoError(t, err)
			require.True(t, ok)

			err = bkt.Delete(ctx, originalPath)
			require.NoError(t, err)

			// Ensure it deleted on originalPath
			ok, err = bkt.Exists(ctx, originalPath)
			require.NoError(t, err)
			require.False(t, ok)

			// Ensure it exists on globalPath
			ok, err = bkt.Exists(ctx, tc.globalpath)
			require.NoError(t, err)
			require.False(t, ok)
		})
	}
}

func TestGlobalMarkersBucket_Delete_ShouldSucceedIfMarkDoesNotExistInTheBlockButExistInTheGlobalLocation(t *testing.T) {
	tests := []struct {
		name  string
		pathF func(ulid.ULID) string
	}{
		{
			name:  metadata.DeletionMarkFilename,
			pathF: BlockDeletionMarkFilepath,
		},
		{
			name:  metadata.NoCompactMarkFilename,
			pathF: NoCompactMarkFilenameMarkFilepath,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)

			ctx := context.Background()
			bkt = BucketWithGlobalMarkers(bkt)

			// Create a mocked block deletion mark in the global location.
			blockID := ulid.MustNew(1, nil)
			globalPath := tc.pathF(blockID)
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
		})
	}
}

func TestGlobalMarkersBucket_isMark(t *testing.T) {
	block1 := ulid.MustNew(1, nil)

	tests := []struct {
		name               string
		expectedOk         bool
		expectedGlobalPath string
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
			name:               block1.String() + "/deletion-mark.json",
			expectedOk:         true,
			expectedGlobalPath: "markers/" + block1.String() + "-deletion-mark.json",
		}, {
			name:               "/path/to/" + block1.String() + "/deletion-mark.json",
			expectedOk:         true,
			expectedGlobalPath: "/path/to/markers/" + block1.String() + "-deletion-mark.json",
		}, {
			name:               block1.String() + "/no-compact-mark.json",
			expectedOk:         true,
			expectedGlobalPath: "markers/" + block1.String() + "-no-compact-mark.json",
		}, {
			name:               "/path/to/" + block1.String() + "/no-compact-mark.json",
			expectedOk:         true,
			expectedGlobalPath: "/path/to/markers/" + block1.String() + "-no-compact-mark.json",
		},
	}

	b := BucketWithGlobalMarkers(nil).(*globalMarkersBucket)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			globalPath, actualOk := b.isMark(tc.name)
			assert.Equal(t, tc.expectedOk, actualOk)
			assert.Equal(t, tc.expectedGlobalPath, globalPath)
		})
	}
}

func TestBucketWithGlobalMarkers_ShouldRetryUpload(t *testing.T) {
	ctx := context.Background()
	block1 := ulid.MustNew(1, nil)

	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)

	// Fail the global markers and the non-global marker
	prefixErrors := []string{"", "marker"}

	tests := []struct {
		mark       string
		globalpath string
	}{
		{
			mark:       metadata.DeletionMarkFilename,
			globalpath: "markers/" + block1.String() + "-deletion-mark.json",
		},
		{
			mark:       metadata.NoCompactMarkFilename,
			globalpath: "markers/" + block1.String() + "-no-compact-mark.json",
		},
	}

	for _, tc := range tests {
		for _, p := range prefixErrors {
			t.Run(tc.mark+"/"+p, func(t *testing.T) {
				mBucket := &cortex_testutil.MockBucketFailure{
					Bucket:         bkt,
					UploadFailures: map[string]error{p: errors.New("test")},
				}
				s3Bkt, _ := s3.NewBucketWithRetries(mBucket, 5, 0, 0, log.NewNopLogger())
				bkt = BucketWithGlobalMarkers(objstore.WithNoopInstr(s3Bkt))
				originalPath := block1.String() + "/" + tc.mark
				err := bkt.Upload(ctx, originalPath, strings.NewReader("{}"))
				require.Equal(t, errors.New("test"), err)
				require.Equal(t, mBucket.UploadCalls.Load(), int32(5))
			})
		}

	}
}

func TestBucketWithGlobalMarkers_ShouldWorkCorrectlyWithBucketMetrics(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	ctx := context.Background()

	// We wrap the underlying filesystem bucket client with metrics,
	// global markers (intentionally in the middle of the chain) and
	// user prefix.
	bkt, _ := cortex_testutil.PrepareFilesystemBucket(t)
	bkt = objstore.WrapWithMetrics(bkt, prometheus.WrapRegistererWithPrefix("thanos_", reg), "")
	bkt = BucketWithGlobalMarkers(bkt)
	userBkt := bucket.NewUserBucketClient("user-1", bkt, nil)

	reader, err := userBkt.Get(ctx, "does-not-exist")
	require.Error(t, err)
	require.Nil(t, reader)
	assert.True(t, bkt.IsObjNotFoundErr(err))

	// Should track the failure.
	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP thanos_objstore_bucket_operation_failures_total Total number of operations against a bucket that failed, but were not expected to fail in certain way from caller perspective. Those errors have to be investigated.
		# TYPE thanos_objstore_bucket_operation_failures_total counter
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="attributes"} 0
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="delete"} 0
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="exists"} 0
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="get"} 1
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="get_range"} 0
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="iter"} 0
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="upload"} 0
		# HELP thanos_objstore_bucket_operations_total Total number of all attempted operations against a bucket.
		# TYPE thanos_objstore_bucket_operations_total counter
		thanos_objstore_bucket_operations_total{bucket="",operation="attributes"} 0
		thanos_objstore_bucket_operations_total{bucket="",operation="delete"} 0
		thanos_objstore_bucket_operations_total{bucket="",operation="exists"} 0
		thanos_objstore_bucket_operations_total{bucket="",operation="get"} 1
		thanos_objstore_bucket_operations_total{bucket="",operation="get_range"} 0
		thanos_objstore_bucket_operations_total{bucket="",operation="iter"} 0
		thanos_objstore_bucket_operations_total{bucket="",operation="upload"} 0
	`),
		"thanos_objstore_bucket_operations_total",
		"thanos_objstore_bucket_operation_failures_total",
	))

	reader, err = userBkt.ReaderWithExpectedErrs(userBkt.IsObjNotFoundErr).Get(ctx, "does-not-exist")
	require.Error(t, err)
	require.Nil(t, reader)
	assert.True(t, bkt.IsObjNotFoundErr(err))

	// Should not track the failure.
	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP thanos_objstore_bucket_operation_failures_total Total number of operations against a bucket that failed, but were not expected to fail in certain way from caller perspective. Those errors have to be investigated.
		# TYPE thanos_objstore_bucket_operation_failures_total counter
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="attributes"} 0
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="delete"} 0
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="exists"} 0
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="get"} 1
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="get_range"} 0
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="iter"} 0
		thanos_objstore_bucket_operation_failures_total{bucket="",operation="upload"} 0
		# HELP thanos_objstore_bucket_operations_total Total number of all attempted operations against a bucket.
		# TYPE thanos_objstore_bucket_operations_total counter
		thanos_objstore_bucket_operations_total{bucket="",operation="attributes"} 0
		thanos_objstore_bucket_operations_total{bucket="",operation="delete"} 0
		thanos_objstore_bucket_operations_total{bucket="",operation="exists"} 0
		thanos_objstore_bucket_operations_total{bucket="",operation="get"} 2
		thanos_objstore_bucket_operations_total{bucket="",operation="get_range"} 0
		thanos_objstore_bucket_operations_total{bucket="",operation="iter"} 0
		thanos_objstore_bucket_operations_total{bucket="",operation="upload"} 0
	`),
		"thanos_objstore_bucket_operations_total",
		"thanos_objstore_bucket_operation_failures_total",
	))
}
