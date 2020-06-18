package compactor

import (
	"context"
	"crypto/rand"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/pkg/storage/backend/filesystem"
	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestBlocksCleaner(t *testing.T) {
	// Create a temporary directory for local storage.
	storageDir, err := ioutil.TempDir(os.TempDir(), "storage")
	require.NoError(t, err)
	defer os.RemoveAll(storageDir) //nolint:errcheck

	// Create a temporary directory for cleaner.
	dataDir, err := ioutil.TempDir(os.TempDir(), "data")
	require.NoError(t, err)
	defer os.RemoveAll(dataDir) //nolint:errcheck

	// Create a bucket client on the local storage.
	bucketClient, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	// Create blocks.
	ctx := context.Background()
	now := time.Now()
	deletionDelay := 12 * time.Hour
	block1 := createTSDBBlock(t, filepath.Join(storageDir, "user-1"), 10, 20, nil)
	block2 := createTSDBBlock(t, filepath.Join(storageDir, "user-1"), 20, 30, nil)
	block3 := createTSDBBlock(t, filepath.Join(storageDir, "user-1"), 30, 40, nil)
	block4 := ulid.MustNew(4, rand.Reader)
	block5 := ulid.MustNew(5, rand.Reader)
	block6 := createTSDBBlock(t, filepath.Join(storageDir, "user-1"), 40, 50, nil)
	createDeletionMark(t, filepath.Join(storageDir, "user-1"), block2, now.Add(-deletionDelay).Add(time.Hour))  // Block hasn't reached the deletion threshold yet.
	createDeletionMark(t, filepath.Join(storageDir, "user-1"), block3, now.Add(-deletionDelay).Add(-time.Hour)) // Block reached the deletion threshold.
	createDeletionMark(t, filepath.Join(storageDir, "user-1"), block4, now.Add(-deletionDelay).Add(time.Hour))  // Partial block hasn't reached the deletion threshold yet.
	createDeletionMark(t, filepath.Join(storageDir, "user-1"), block5, now.Add(-deletionDelay).Add(-time.Hour)) // Partial block reached the deletion threshold.
	require.NoError(t, bucketClient.Delete(ctx, path.Join("user-1", block6.String(), metadata.MetaFilename)))   // Partial block without deletion mark.

	cfg := BlocksCleanerConfig{
		DataDir:             dataDir,
		MetaSyncConcurrency: 10,
		DeletionDelay:       deletionDelay,
		CleanupInterval:     time.Minute,
	}

	logger := log.NewNopLogger()
	scanner := NewUsersScanner(bucketClient, func(_ string) (bool, error) { return true, nil }, logger)

	cleaner := NewBlocksCleaner(cfg, bucketClient, scanner, logger, nil)
	require.NoError(t, services.StartAndAwaitRunning(ctx, cleaner))
	defer services.StopAndAwaitTerminated(ctx, cleaner) //nolint:errcheck

	// Check the storage to ensure only the block which has reached the deletion threshold
	// has been effectively deleted.
	exists, err := bucketClient.Exists(ctx, path.Join("user-1", block1.String(), metadata.MetaFilename))
	require.NoError(t, err)
	assert.True(t, exists)

	exists, err = bucketClient.Exists(ctx, path.Join("user-1", block2.String(), metadata.MetaFilename))
	require.NoError(t, err)
	assert.True(t, exists)

	exists, err = bucketClient.Exists(ctx, path.Join("user-1", block3.String(), metadata.MetaFilename))
	require.NoError(t, err)
	assert.False(t, exists)

	// Should delete a partial block with deletion mark who hasn't reached the deletion threshold yet.
	exists, err = bucketClient.Exists(ctx, path.Join("user-1", block4.String(), metadata.DeletionMarkFilename))
	require.NoError(t, err)
	assert.False(t, exists)

	// Should delete a partial block with deletion mark who has reached the deletion threshold.
	exists, err = bucketClient.Exists(ctx, path.Join("user-1", block5.String(), metadata.DeletionMarkFilename))
	require.NoError(t, err)
	assert.False(t, exists)

	// Should not delete a partial block without deletion mark.
	exists, err = bucketClient.Exists(ctx, path.Join("user-1", block6.String(), "index"))
	require.NoError(t, err)
	assert.True(t, exists)

	assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.runsStarted))
	assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.runsCompleted))
	assert.Equal(t, float64(0), testutil.ToFloat64(cleaner.runsFailed))
	assert.Equal(t, float64(3), testutil.ToFloat64(cleaner.blocksCleanedTotal))
	assert.Equal(t, float64(0), testutil.ToFloat64(cleaner.blocksFailedTotal))
}
