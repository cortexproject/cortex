package compactor

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	cortex_testutil "github.com/cortexproject/cortex/pkg/storage/tsdb/testutil"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
)

type testBlocksCleanerOptions struct {
	concurrency             int
	markersMigrationEnabled bool
	tenantDeletionDelay     time.Duration
	user4FilesExist         bool // User 4 has "FinishedTime" in tenant deletion marker set to "1h" ago.
}

func (o testBlocksCleanerOptions) String() string {
	return fmt.Sprintf("concurrency=%d, markers migration enabled=%v, tenant deletion delay=%v",
		o.concurrency, o.markersMigrationEnabled, o.tenantDeletionDelay)
}

func TestBlocksCleaner(t *testing.T) {
	for _, options := range []testBlocksCleanerOptions{
		{concurrency: 1, tenantDeletionDelay: 0, user4FilesExist: false},
		{concurrency: 1, tenantDeletionDelay: 2 * time.Hour, user4FilesExist: true},
		{concurrency: 1, markersMigrationEnabled: true},
		{concurrency: 2},
		{concurrency: 10},
	} {
		options := options

		t.Run(options.String(), func(t *testing.T) {
			t.Parallel()
			testBlocksCleanerWithOptions(t, options)
		})
	}
}

func testBlocksCleanerWithOptions(t *testing.T, options testBlocksCleanerOptions) {
	bucketClient, _ := cortex_testutil.PrepareFilesystemBucket(t)

	// If the markers migration is enabled, then we create the fixture blocks without
	// writing the deletion marks in the global location, because they will be migrated
	// at statup.
	if !options.markersMigrationEnabled {
		bucketClient = bucketindex.BucketWithGlobalMarkers(bucketClient)
	}

	// Create blocks.
	ctx := context.Background()
	now := time.Now()
	deletionDelay := 12 * time.Hour
	block1 := createTSDBBlock(t, bucketClient, "user-1", 10, 20, nil)
	block2 := createTSDBBlock(t, bucketClient, "user-1", 20, 30, nil)
	block3 := createTSDBBlock(t, bucketClient, "user-1", 30, 40, nil)
	block4 := ulid.MustNew(4, rand.Reader)
	block5 := ulid.MustNew(5, rand.Reader)
	block6 := createTSDBBlock(t, bucketClient, "user-1", 40, 50, nil)
	block7 := createTSDBBlock(t, bucketClient, "user-2", 10, 20, nil)
	block8 := createTSDBBlock(t, bucketClient, "user-2", 40, 50, nil)
	createDeletionMark(t, bucketClient, "user-1", block2, now.Add(-deletionDelay).Add(time.Hour))             // Block hasn't reached the deletion threshold yet.
	createDeletionMark(t, bucketClient, "user-1", block3, now.Add(-deletionDelay).Add(-time.Hour))            // Block reached the deletion threshold.
	createDeletionMark(t, bucketClient, "user-1", block4, now.Add(-deletionDelay).Add(time.Hour))             // Partial block hasn't reached the deletion threshold yet.
	createDeletionMark(t, bucketClient, "user-1", block5, now.Add(-deletionDelay).Add(-time.Hour))            // Partial block reached the deletion threshold.
	require.NoError(t, bucketClient.Delete(ctx, path.Join("user-1", block6.String(), metadata.MetaFilename))) // Partial block without deletion mark.
	createDeletionMark(t, bucketClient, "user-2", block7, now.Add(-deletionDelay).Add(-time.Hour))            // Block reached the deletion threshold.

	// Blocks for user-3, marked for deletion.
	require.NoError(t, tsdb.WriteTenantDeletionMark(context.Background(), bucketClient, "user-3", nil, tsdb.NewTenantDeletionMark(time.Now())))
	block9 := createTSDBBlock(t, bucketClient, "user-3", 10, 30, nil)
	block10 := createTSDBBlock(t, bucketClient, "user-3", 30, 50, nil)

	// User-4 with no more blocks, but couple of mark and debug files. Should be fully deleted.
	user4Mark := tsdb.NewTenantDeletionMark(time.Now())
	user4Mark.FinishedTime = time.Now().Unix() - 60 // Set to check final user cleanup.
	require.NoError(t, tsdb.WriteTenantDeletionMark(context.Background(), bucketClient, "user-4", nil, user4Mark))
	user4DebugMetaFile := path.Join("user-4", block.DebugMetas, "meta.json")
	require.NoError(t, bucketClient.Upload(context.Background(), user4DebugMetaFile, strings.NewReader("some random content here")))

	// The fixtures have been created. If the bucket client wasn't wrapped to write
	// deletion marks to the global location too, then this is the right time to do it.
	if options.markersMigrationEnabled {
		bucketClient = bucketindex.BucketWithGlobalMarkers(bucketClient)
	}

	cfg := BlocksCleanerConfig{
		DeletionDelay:                      deletionDelay,
		CleanupInterval:                    time.Minute,
		CleanupConcurrency:                 options.concurrency,
		BlockDeletionMarksMigrationEnabled: options.markersMigrationEnabled,
		TenantCleanupDelay:                 options.tenantDeletionDelay,
	}

	reg := prometheus.NewPedanticRegistry()
	logger := log.NewNopLogger()
	scanner := tsdb.NewUsersScanner(bucketClient, tsdb.AllUsers, logger)

	cleaner := NewBlocksCleaner(cfg, bucketClient, scanner, nil, logger, reg)
	require.NoError(t, services.StartAndAwaitRunning(ctx, cleaner))
	defer services.StopAndAwaitTerminated(ctx, cleaner) //nolint:errcheck

	for _, tc := range []struct {
		path           string
		expectedExists bool
	}{
		// Check the storage to ensure only the block which has reached the deletion threshold
		// has been effectively deleted.
		{path: path.Join("user-1", block1.String(), metadata.MetaFilename), expectedExists: true},
		{path: path.Join("user-1", block3.String(), metadata.MetaFilename), expectedExists: false},
		{path: path.Join("user-2", block7.String(), metadata.MetaFilename), expectedExists: false},
		{path: path.Join("user-2", block8.String(), metadata.MetaFilename), expectedExists: true},
		// Should not delete a block with deletion mark who hasn't reached the deletion threshold yet.
		{path: path.Join("user-1", block2.String(), metadata.MetaFilename), expectedExists: true},
		{path: path.Join("user-1", bucketindex.BlockDeletionMarkFilepath(block2)), expectedExists: true},
		// Should delete a partial block with deletion mark who hasn't reached the deletion threshold yet.
		{path: path.Join("user-1", block4.String(), metadata.DeletionMarkFilename), expectedExists: false},
		{path: path.Join("user-1", bucketindex.BlockDeletionMarkFilepath(block4)), expectedExists: false},
		// Should delete a partial block with deletion mark who has reached the deletion threshold.
		{path: path.Join("user-1", block5.String(), metadata.DeletionMarkFilename), expectedExists: false},
		{path: path.Join("user-1", bucketindex.BlockDeletionMarkFilepath(block5)), expectedExists: false},
		// Should not delete a partial block without deletion mark.
		{path: path.Join("user-1", block6.String(), "index"), expectedExists: true},
		// Should completely delete blocks for user-3, marked for deletion
		{path: path.Join("user-3", block9.String(), metadata.MetaFilename), expectedExists: false},
		{path: path.Join("user-3", block9.String(), "index"), expectedExists: false},
		{path: path.Join("user-3", block10.String(), metadata.MetaFilename), expectedExists: false},
		{path: path.Join("user-3", block10.String(), "index"), expectedExists: false},
		// Tenant deletion mark is not removed.
		{path: path.Join("user-3", tsdb.TenantDeletionMarkPath), expectedExists: true},
		// User-4 is removed fully.
		{path: path.Join("user-4", tsdb.TenantDeletionMarkPath), expectedExists: options.user4FilesExist},
		{path: path.Join("user-4", block.DebugMetas, "meta.json"), expectedExists: options.user4FilesExist},
	} {
		exists, err := bucketClient.Exists(ctx, tc.path)
		require.NoError(t, err)
		assert.Equal(t, tc.expectedExists, exists, tc.path)
	}

	assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.runsStarted))
	assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.runsCompleted))
	assert.Equal(t, float64(0), testutil.ToFloat64(cleaner.runsFailed))
	assert.Equal(t, float64(6), testutil.ToFloat64(cleaner.blocksCleanedTotal))
	assert.Equal(t, float64(0), testutil.ToFloat64(cleaner.blocksFailedTotal))

	// Check the updated bucket index.
	for _, tc := range []struct {
		userID         string
		expectedIndex  bool
		expectedBlocks []ulid.ULID
		expectedMarks  []ulid.ULID
	}{
		{
			userID:         "user-1",
			expectedIndex:  true,
			expectedBlocks: []ulid.ULID{block1, block2 /* deleted: block3, block4, block5, partial: block6 */},
			expectedMarks:  []ulid.ULID{block2},
		}, {
			userID:         "user-2",
			expectedIndex:  true,
			expectedBlocks: []ulid.ULID{block8},
			expectedMarks:  []ulid.ULID{},
		}, {
			userID:        "user-3",
			expectedIndex: false,
		},
	} {
		idx, err := bucketindex.ReadIndex(ctx, bucketClient, tc.userID, nil, logger)
		if !tc.expectedIndex {
			assert.Equal(t, bucketindex.ErrIndexNotFound, err)
			continue
		}

		require.NoError(t, err)
		assert.ElementsMatch(t, tc.expectedBlocks, idx.Blocks.GetULIDs())
		assert.ElementsMatch(t, tc.expectedMarks, idx.BlockDeletionMarks.GetULIDs())
	}

	assert.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_bucket_blocks_count Total number of blocks in the bucket. Includes blocks marked for deletion, but not partial blocks.
		# TYPE cortex_bucket_blocks_count gauge
		cortex_bucket_blocks_count{user="user-1"} 2
		cortex_bucket_blocks_count{user="user-2"} 1
		# HELP cortex_bucket_blocks_marked_for_deletion_count Total number of blocks marked for deletion in the bucket.
		# TYPE cortex_bucket_blocks_marked_for_deletion_count gauge
		cortex_bucket_blocks_marked_for_deletion_count{user="user-1"} 1
		cortex_bucket_blocks_marked_for_deletion_count{user="user-2"} 0
		# HELP cortex_bucket_blocks_partials_count Total number of partial blocks.
		# TYPE cortex_bucket_blocks_partials_count gauge
		cortex_bucket_blocks_partials_count{user="user-1"} 2
		cortex_bucket_blocks_partials_count{user="user-2"} 0
	`),
		"cortex_bucket_blocks_count",
		"cortex_bucket_blocks_marked_for_deletion_count",
		"cortex_bucket_blocks_partials_count",
	))
}

func TestBlocksCleaner_ShouldContinueOnBlockDeletionFailure(t *testing.T) {
	const userID = "user-1"

	bucketClient, _ := cortex_testutil.PrepareFilesystemBucket(t)
	bucketClient = bucketindex.BucketWithGlobalMarkers(bucketClient)

	// Create blocks.
	ctx := context.Background()
	now := time.Now()
	deletionDelay := 12 * time.Hour
	block1 := createTSDBBlock(t, bucketClient, userID, 10, 20, nil)
	block2 := createTSDBBlock(t, bucketClient, userID, 20, 30, nil)
	block3 := createTSDBBlock(t, bucketClient, userID, 30, 40, nil)
	block4 := createTSDBBlock(t, bucketClient, userID, 40, 50, nil)
	createDeletionMark(t, bucketClient, userID, block2, now.Add(-deletionDelay).Add(-time.Hour))
	createDeletionMark(t, bucketClient, userID, block3, now.Add(-deletionDelay).Add(-time.Hour))
	createDeletionMark(t, bucketClient, userID, block4, now.Add(-deletionDelay).Add(-time.Hour))

	// To emulate a failure deleting a block, we wrap the bucket client in a mocked one.
	bucketClient = &mockBucketFailure{
		Bucket:         bucketClient,
		DeleteFailures: []string{path.Join(userID, block3.String(), metadata.MetaFilename)},
	}

	cfg := BlocksCleanerConfig{
		DeletionDelay:      deletionDelay,
		CleanupInterval:    time.Minute,
		CleanupConcurrency: 1,
	}

	logger := log.NewNopLogger()
	scanner := tsdb.NewUsersScanner(bucketClient, tsdb.AllUsers, logger)

	cleaner := NewBlocksCleaner(cfg, bucketClient, scanner, nil, logger, nil)
	require.NoError(t, services.StartAndAwaitRunning(ctx, cleaner))
	defer services.StopAndAwaitTerminated(ctx, cleaner) //nolint:errcheck

	for _, tc := range []struct {
		path           string
		expectedExists bool
	}{
		{path: path.Join(userID, block1.String(), metadata.MetaFilename), expectedExists: true},
		{path: path.Join(userID, block2.String(), metadata.MetaFilename), expectedExists: false},
		{path: path.Join(userID, block3.String(), metadata.MetaFilename), expectedExists: true},
		{path: path.Join(userID, block4.String(), metadata.MetaFilename), expectedExists: false},
	} {
		exists, err := bucketClient.Exists(ctx, tc.path)
		require.NoError(t, err)
		assert.Equal(t, tc.expectedExists, exists, tc.path)
	}

	assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.runsStarted))
	assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.runsCompleted))
	assert.Equal(t, float64(0), testutil.ToFloat64(cleaner.runsFailed))
	assert.Equal(t, float64(2), testutil.ToFloat64(cleaner.blocksCleanedTotal))
	assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.blocksFailedTotal))

	// Check the updated bucket index.
	idx, err := bucketindex.ReadIndex(ctx, bucketClient, userID, nil, logger)
	require.NoError(t, err)
	assert.ElementsMatch(t, []ulid.ULID{block1, block3}, idx.Blocks.GetULIDs())
	assert.ElementsMatch(t, []ulid.ULID{block3}, idx.BlockDeletionMarks.GetULIDs())
}

func TestBlocksCleaner_ShouldRebuildBucketIndexOnCorruptedOne(t *testing.T) {
	const userID = "user-1"

	bucketClient, _ := cortex_testutil.PrepareFilesystemBucket(t)
	bucketClient = bucketindex.BucketWithGlobalMarkers(bucketClient)

	// Create blocks.
	ctx := context.Background()
	now := time.Now()
	deletionDelay := 12 * time.Hour
	block1 := createTSDBBlock(t, bucketClient, userID, 10, 20, nil)
	block2 := createTSDBBlock(t, bucketClient, userID, 20, 30, nil)
	block3 := createTSDBBlock(t, bucketClient, userID, 30, 40, nil)
	createDeletionMark(t, bucketClient, userID, block2, now.Add(-deletionDelay).Add(-time.Hour))
	createDeletionMark(t, bucketClient, userID, block3, now.Add(-deletionDelay).Add(time.Hour))

	// Write a corrupted bucket index.
	require.NoError(t, bucketClient.Upload(ctx, path.Join(userID, bucketindex.IndexCompressedFilename), strings.NewReader("invalid!}")))

	cfg := BlocksCleanerConfig{
		DeletionDelay:      deletionDelay,
		CleanupInterval:    time.Minute,
		CleanupConcurrency: 1,
	}

	logger := log.NewNopLogger()
	scanner := tsdb.NewUsersScanner(bucketClient, tsdb.AllUsers, logger)

	cleaner := NewBlocksCleaner(cfg, bucketClient, scanner, nil, logger, nil)
	require.NoError(t, services.StartAndAwaitRunning(ctx, cleaner))
	defer services.StopAndAwaitTerminated(ctx, cleaner) //nolint:errcheck

	for _, tc := range []struct {
		path           string
		expectedExists bool
	}{
		{path: path.Join(userID, block1.String(), metadata.MetaFilename), expectedExists: true},
		{path: path.Join(userID, block2.String(), metadata.MetaFilename), expectedExists: false},
		{path: path.Join(userID, block3.String(), metadata.MetaFilename), expectedExists: true},
	} {
		exists, err := bucketClient.Exists(ctx, tc.path)
		require.NoError(t, err)
		assert.Equal(t, tc.expectedExists, exists, tc.path)
	}

	assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.runsStarted))
	assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.runsCompleted))
	assert.Equal(t, float64(0), testutil.ToFloat64(cleaner.runsFailed))
	assert.Equal(t, float64(1), testutil.ToFloat64(cleaner.blocksCleanedTotal))
	assert.Equal(t, float64(0), testutil.ToFloat64(cleaner.blocksFailedTotal))

	// Check the updated bucket index.
	idx, err := bucketindex.ReadIndex(ctx, bucketClient, userID, nil, logger)
	require.NoError(t, err)
	assert.ElementsMatch(t, []ulid.ULID{block1, block3}, idx.Blocks.GetULIDs())
	assert.ElementsMatch(t, []ulid.ULID{block3}, idx.BlockDeletionMarks.GetULIDs())
}

func TestBlocksCleaner_ShouldRemoveMetricsForTenantsNotBelongingAnymoreToTheShard(t *testing.T) {
	bucketClient, _ := cortex_testutil.PrepareFilesystemBucket(t)
	bucketClient = bucketindex.BucketWithGlobalMarkers(bucketClient)

	// Create blocks.
	createTSDBBlock(t, bucketClient, "user-1", 10, 20, nil)
	createTSDBBlock(t, bucketClient, "user-1", 20, 30, nil)
	createTSDBBlock(t, bucketClient, "user-2", 30, 40, nil)

	cfg := BlocksCleanerConfig{
		DeletionDelay:      time.Hour,
		CleanupInterval:    time.Minute,
		CleanupConcurrency: 1,
	}

	ctx := context.Background()
	logger := log.NewNopLogger()
	reg := prometheus.NewPedanticRegistry()
	scanner := tsdb.NewUsersScanner(bucketClient, tsdb.AllUsers, logger)

	cleaner := NewBlocksCleaner(cfg, bucketClient, scanner, nil, logger, reg)
	require.NoError(t, cleaner.cleanUsers(ctx, true))

	assert.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_bucket_blocks_count Total number of blocks in the bucket. Includes blocks marked for deletion, but not partial blocks.
		# TYPE cortex_bucket_blocks_count gauge
		cortex_bucket_blocks_count{user="user-1"} 2
		cortex_bucket_blocks_count{user="user-2"} 1
		# HELP cortex_bucket_blocks_marked_for_deletion_count Total number of blocks marked for deletion in the bucket.
		# TYPE cortex_bucket_blocks_marked_for_deletion_count gauge
		cortex_bucket_blocks_marked_for_deletion_count{user="user-1"} 0
		cortex_bucket_blocks_marked_for_deletion_count{user="user-2"} 0
		# HELP cortex_bucket_blocks_partials_count Total number of partial blocks.
		# TYPE cortex_bucket_blocks_partials_count gauge
		cortex_bucket_blocks_partials_count{user="user-1"} 0
		cortex_bucket_blocks_partials_count{user="user-2"} 0
	`),
		"cortex_bucket_blocks_count",
		"cortex_bucket_blocks_marked_for_deletion_count",
		"cortex_bucket_blocks_partials_count",
	))

	// Override the users scanner to reconfigure it to only return a subset of users.
	cleaner.usersScanner = tsdb.NewUsersScanner(bucketClient, func(userID string) (bool, error) { return userID == "user-1", nil }, logger)

	// Create new blocks, to double check expected metrics have changed.
	createTSDBBlock(t, bucketClient, "user-1", 40, 50, nil)
	createTSDBBlock(t, bucketClient, "user-2", 50, 60, nil)

	require.NoError(t, cleaner.cleanUsers(ctx, false))

	assert.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_bucket_blocks_count Total number of blocks in the bucket. Includes blocks marked for deletion, but not partial blocks.
		# TYPE cortex_bucket_blocks_count gauge
		cortex_bucket_blocks_count{user="user-1"} 3
		# HELP cortex_bucket_blocks_marked_for_deletion_count Total number of blocks marked for deletion in the bucket.
		# TYPE cortex_bucket_blocks_marked_for_deletion_count gauge
		cortex_bucket_blocks_marked_for_deletion_count{user="user-1"} 0
		# HELP cortex_bucket_blocks_partials_count Total number of partial blocks.
		# TYPE cortex_bucket_blocks_partials_count gauge
		cortex_bucket_blocks_partials_count{user="user-1"} 0
	`),
		"cortex_bucket_blocks_count",
		"cortex_bucket_blocks_marked_for_deletion_count",
		"cortex_bucket_blocks_partials_count",
	))
}

type mockBucketFailure struct {
	objstore.Bucket

	DeleteFailures []string
}

func (m *mockBucketFailure) Delete(ctx context.Context, name string) error {
	if util.StringsContain(m.DeleteFailures, name) {
		return errors.New("mocked delete failure")
	}
	return m.Bucket.Delete(ctx, name)
}
