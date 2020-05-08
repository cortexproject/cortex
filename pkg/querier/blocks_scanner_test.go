package querier

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/storage/backend/filesystem"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestBlocksScanner_InitialScan(t *testing.T) {
	ctx := context.Background()
	s, bucket, _, reg, cleanup := prepareBlocksScanner(t, prepareBlocksScannerConfig())
	defer cleanup()

	user1Block1 := mockStorageBlock(t, bucket, "user-1", 10, 20)
	user1Block2 := mockStorageBlock(t, bucket, "user-1", 20, 30)
	user2Block1 := mockStorageBlock(t, bucket, "user-2", 10, 20)

	require.NoError(t, services.StartAndAwaitRunning(ctx, s))

	blocks, err := s.GetBlocks("user-1", 0, 30)
	require.NoError(t, err)
	require.Equal(t, 2, len(blocks))
	assert.Equal(t, user1Block2.ULID, blocks[0].ULID)
	assert.Equal(t, user1Block1.ULID, blocks[1].ULID)

	blocks, err = s.GetBlocks("user-2", 0, 30)
	require.NoError(t, err)
	require.Equal(t, 1, len(blocks))
	assert.Equal(t, user2Block1.ULID, blocks[0].ULID)

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_querier_blocks_meta_syncs_total Total blocks metadata synchronization attempts
		# TYPE cortex_querier_blocks_meta_syncs_total counter
		cortex_querier_blocks_meta_syncs_total 2

		# HELP cortex_querier_blocks_meta_sync_failures_total Total blocks metadata synchronization failures
		# TYPE cortex_querier_blocks_meta_sync_failures_total counter
		cortex_querier_blocks_meta_sync_failures_total 0

		# HELP cortex_querier_blocks_meta_sync_consistency_delay_seconds Configured consistency delay in seconds.
		# TYPE cortex_querier_blocks_meta_sync_consistency_delay_seconds gauge
		cortex_querier_blocks_meta_sync_consistency_delay_seconds 0
	`),
		"cortex_querier_blocks_meta_syncs_total",
		"cortex_querier_blocks_meta_sync_failures_total",
		"cortex_querier_blocks_meta_sync_consistency_delay_seconds",
	))
}

func TestBlocksScanner_InitialScanFailure(t *testing.T) {
	cacheDir, err := ioutil.TempDir(os.TempDir(), "blocks-scanner-test-cache")
	require.NoError(t, err)
	defer os.RemoveAll(cacheDir) //nolint: errcheck

	ctx := context.Background()
	bucket := &cortex_tsdb.BucketClientMock{}
	reg := prometheus.NewPedanticRegistry()

	cfg := prepareBlocksScannerConfig()
	cfg.CacheDir = cacheDir

	s := NewBlocksScanner(cfg, bucket, log.NewNopLogger(), reg)
	defer func() {
		s.StopAsync()
		s.AwaitTerminated(context.Background()) //nolint: errcheck
	}()

	// Mock the storage to simulate a failure when reading objects.
	bucket.MockIter("", []string{"user-1"}, nil)
	bucket.MockIter("user-1/", []string{"user-1/01DTVP434PA9VFXSW2JKB3392D"}, nil)
	bucket.MockGet("user-1/01DTVP434PA9VFXSW2JKB3392D/meta.json", "invalid", errors.New("mocked error"))

	require.NoError(t, s.StartAsync(ctx))
	require.Error(t, s.AwaitRunning(ctx))

	blocks, err := s.GetBlocks("user-1", 0, 30)
	assert.Equal(t, errBlocksScannerNotRunning, err)
	assert.Nil(t, blocks)

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_querier_blocks_meta_syncs_total Total blocks metadata synchronization attempts
		# TYPE cortex_querier_blocks_meta_syncs_total counter
		cortex_querier_blocks_meta_syncs_total 3

		# HELP cortex_querier_blocks_meta_sync_failures_total Total blocks metadata synchronization failures
		# TYPE cortex_querier_blocks_meta_sync_failures_total counter
		cortex_querier_blocks_meta_sync_failures_total 3

		# HELP cortex_querier_blocks_meta_sync_consistency_delay_seconds Configured consistency delay in seconds.
		# TYPE cortex_querier_blocks_meta_sync_consistency_delay_seconds gauge
		cortex_querier_blocks_meta_sync_consistency_delay_seconds 0
	`),
		"cortex_querier_blocks_meta_syncs_total",
		"cortex_querier_blocks_meta_sync_failures_total",
		"cortex_querier_blocks_meta_sync_consistency_delay_seconds",
	))
}

func TestBlocksScanner_PeriodicScanFindsNewUser(t *testing.T) {
	ctx := context.Background()
	s, bucket, _, _, cleanup := prepareBlocksScanner(t, prepareBlocksScannerConfig())
	defer cleanup()

	require.NoError(t, services.StartAndAwaitRunning(ctx, s))

	blocks, err := s.GetBlocks("user-1", 0, 30)
	require.NoError(t, err)
	require.Equal(t, 0, len(blocks))

	block1 := mockStorageBlock(t, bucket, "user-1", 10, 20)
	block2 := mockStorageBlock(t, bucket, "user-1", 20, 30)

	// Trigger a periodic sync
	require.NoError(t, s.scan(ctx))

	blocks, err = s.GetBlocks("user-1", 0, 30)
	require.NoError(t, err)
	require.Equal(t, 2, len(blocks))
	assert.Equal(t, block2.ULID, blocks[0].ULID)
	assert.Equal(t, block1.ULID, blocks[1].ULID)
}

func TestBlocksScanner_PeriodicScanFindsNewBlock(t *testing.T) {
	ctx := context.Background()
	s, bucket, _, _, cleanup := prepareBlocksScanner(t, prepareBlocksScannerConfig())
	defer cleanup()

	block1 := mockStorageBlock(t, bucket, "user-1", 10, 20)

	require.NoError(t, services.StartAndAwaitRunning(ctx, s))

	blocks, err := s.GetBlocks("user-1", 0, 30)
	require.NoError(t, err)
	require.Equal(t, 1, len(blocks))
	assert.Equal(t, block1.ULID, blocks[0].ULID)

	block2 := mockStorageBlock(t, bucket, "user-1", 20, 30)

	// Trigger a periodic sync
	require.NoError(t, s.scan(ctx))

	blocks, err = s.GetBlocks("user-1", 0, 30)
	require.NoError(t, err)
	require.Equal(t, 2, len(blocks))
	assert.Equal(t, block2.ULID, blocks[0].ULID)
	assert.Equal(t, block1.ULID, blocks[1].ULID)
}

func TestBlocksScanner_PeriodicScanFindsDeletedBlock(t *testing.T) {
	ctx := context.Background()
	s, bucket, _, _, cleanup := prepareBlocksScanner(t, prepareBlocksScannerConfig())
	defer cleanup()

	block1 := mockStorageBlock(t, bucket, "user-1", 10, 20)
	block2 := mockStorageBlock(t, bucket, "user-1", 20, 30)

	require.NoError(t, services.StartAndAwaitRunning(ctx, s))

	blocks, err := s.GetBlocks("user-1", 0, 30)
	require.NoError(t, err)
	require.Equal(t, 2, len(blocks))
	assert.Equal(t, block2.ULID, blocks[0].ULID)
	assert.Equal(t, block1.ULID, blocks[1].ULID)

	require.NoError(t, bucket.Delete(ctx, fmt.Sprintf("%s/%s", "user-1", block1.ULID.String())))

	// Trigger a periodic sync
	require.NoError(t, s.scan(ctx))

	blocks, err = s.GetBlocks("user-1", 0, 30)
	require.NoError(t, err)
	require.Equal(t, 1, len(blocks))
	assert.Equal(t, block2.ULID, blocks[0].ULID)
}

func TestBlocksScanner_PeriodicScanFindsDeletedUser(t *testing.T) {
	ctx := context.Background()
	s, bucket, _, _, cleanup := prepareBlocksScanner(t, prepareBlocksScannerConfig())
	defer cleanup()

	block1 := mockStorageBlock(t, bucket, "user-1", 10, 20)
	block2 := mockStorageBlock(t, bucket, "user-1", 20, 30)

	require.NoError(t, services.StartAndAwaitRunning(ctx, s))

	blocks, err := s.GetBlocks("user-1", 0, 30)
	require.NoError(t, err)
	require.Equal(t, 2, len(blocks))
	assert.Equal(t, block2.ULID, blocks[0].ULID)
	assert.Equal(t, block1.ULID, blocks[1].ULID)

	require.NoError(t, bucket.Delete(ctx, "user-1"))

	// Trigger a periodic sync
	require.NoError(t, s.scan(ctx))

	blocks, err = s.GetBlocks("user-1", 0, 30)
	require.NoError(t, err)
	require.Equal(t, 0, len(blocks))
}

func TestBlocksScanner_PeriodicScanFindsUserWhichWasPreviouslyDeleted(t *testing.T) {
	ctx := context.Background()
	s, bucket, _, _, cleanup := prepareBlocksScanner(t, prepareBlocksScannerConfig())
	defer cleanup()

	block1 := mockStorageBlock(t, bucket, "user-1", 10, 20)
	block2 := mockStorageBlock(t, bucket, "user-1", 20, 30)

	require.NoError(t, services.StartAndAwaitRunning(ctx, s))

	blocks, err := s.GetBlocks("user-1", 0, 40)
	require.NoError(t, err)
	require.Equal(t, 2, len(blocks))
	assert.Equal(t, block2.ULID, blocks[0].ULID)
	assert.Equal(t, block1.ULID, blocks[1].ULID)

	require.NoError(t, bucket.Delete(ctx, "user-1"))

	// Trigger a periodic sync
	require.NoError(t, s.scan(ctx))

	blocks, err = s.GetBlocks("user-1", 0, 40)
	require.NoError(t, err)
	require.Equal(t, 0, len(blocks))

	block3 := mockStorageBlock(t, bucket, "user-1", 30, 40)

	// Trigger a periodic sync
	require.NoError(t, s.scan(ctx))

	blocks, err = s.GetBlocks("user-1", 0, 40)
	require.NoError(t, err)
	require.Equal(t, 1, len(blocks))
	assert.Equal(t, block3.ULID, blocks[0].ULID)
}

func TestBlocksScanner_GetBlocks(t *testing.T) {
	ctx := context.Background()
	s, bucket, _, _, cleanup := prepareBlocksScanner(t, prepareBlocksScannerConfig())
	defer cleanup()

	block1 := mockStorageBlock(t, bucket, "user-1", 10, 15)
	block2 := mockStorageBlock(t, bucket, "user-1", 12, 20)
	block3 := mockStorageBlock(t, bucket, "user-1", 20, 30)
	block4 := mockStorageBlock(t, bucket, "user-1", 30, 40)

	require.NoError(t, services.StartAndAwaitRunning(ctx, s))

	tests := map[string]struct {
		minT     int64
		maxT     int64
		expected []tsdb.BlockMeta
	}{
		"no matching block because the range is too low": {
			minT: 0,
			maxT: 5,
		},
		"no matching block because the range is too high": {
			minT: 50,
			maxT: 60,
		},
		"matching all blocks": {
			minT:     0,
			maxT:     60,
			expected: []tsdb.BlockMeta{block4, block3, block2, block1},
		},
		"query range starting at a block maxT": {
			minT:     block3.MaxTime,
			maxT:     60,
			expected: []tsdb.BlockMeta{block4},
		},
		"query range ending at a block minT": {
			minT:     block3.MinTime,
			maxT:     block4.MinTime,
			expected: []tsdb.BlockMeta{block4, block3},
		},
		"query range within a single block": {
			minT:     block3.MinTime + 2,
			maxT:     block3.MaxTime - 2,
			expected: []tsdb.BlockMeta{block3},
		},
		"query range within multiple blocks": {
			minT:     13,
			maxT:     16,
			expected: []tsdb.BlockMeta{block2, block1},
		},
		"query range matching exactly a single block": {
			minT:     block3.MinTime,
			maxT:     block3.MaxTime - 1,
			expected: []tsdb.BlockMeta{block3},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actual, err := s.GetBlocks("user-1", testData.minT, testData.maxT)
			require.NoError(t, err)
			require.Equal(t, len(testData.expected), len(actual))

			for i, expectedBlock := range testData.expected {
				assert.Equal(t, expectedBlock.ULID, actual[i].ULID)
			}
		})
	}
}

func prepareBlocksScanner(t *testing.T, cfg BlocksScannerConfig) (*BlocksScanner, objstore.Bucket, string, *prometheus.Registry, func()) {
	cacheDir, err := ioutil.TempDir(os.TempDir(), "blocks-scanner-test-cache")
	require.NoError(t, err)

	storageDir, err := ioutil.TempDir(os.TempDir(), "blocks-scanner-test-storage")
	require.NoError(t, err)

	bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	reg := prometheus.NewPedanticRegistry()
	cfg.CacheDir = cacheDir
	s := NewBlocksScanner(cfg, bucket, log.NewNopLogger(), reg)

	cleanup := func() {
		s.StopAsync()
		s.AwaitTerminated(context.Background()) //nolint: errcheck
		require.NoError(t, os.RemoveAll(cacheDir))
		require.NoError(t, os.RemoveAll(storageDir))
	}

	return s, bucket, storageDir, reg, cleanup
}

func prepareBlocksScannerConfig() BlocksScannerConfig {
	return BlocksScannerConfig{
		ScanInterval:             time.Minute,
		TenantsConcurrency:       10,
		MetasConcurrency:         10,
		ConsistencyDelay:         0,
		IgnoreDeletionMarksDelay: 0,
	}
}

func mockStorageBlock(t *testing.T, bucket objstore.Bucket, userID string, minT, maxT int64) tsdb.BlockMeta {
	// Generate a block ID whose timestamp matches the maxT (for simplicity we assume it
	// has been compacted and shipped in zero time, even if not realistic).
	id := ulid.MustNew(uint64(maxT), rand.Reader)

	meta := tsdb.BlockMeta{
		Version: 1,
		ULID:    id,
		MinTime: minT,
		MaxTime: maxT,
		Compaction: tsdb.BlockMetaCompaction{
			Level:   1,
			Sources: []ulid.ULID{id},
		},
	}

	metaContent, err := json.Marshal(meta)
	if err != nil {
		panic("failed to marshal mocked block meta")
	}

	metaContentReader := strings.NewReader(string(metaContent))
	metaPath := fmt.Sprintf("%s/%s/meta.json", userID, id.String())
	require.NoError(t, bucket.Upload(context.Background(), metaPath, metaContentReader))

	return meta
}
