package testutil

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/cortexproject/cortex/pkg/storage/bucket/filesystem"
)

func PrepareFilesystemBucket(t testing.TB) (objstore.Bucket, string) {
	storageDir, err := os.MkdirTemp(os.TempDir(), "bucket")
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(storageDir))
	})

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	return objstore.BucketWithMetrics("test", bkt, nil), storageDir
}
