package querier

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/weaveworks/common/logging"

	"github.com/cortexproject/cortex/pkg/storage/backend/filesystem"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

func TestBucketStores_InitialSync(t *testing.T) {
	userToMetric := map[string]string{
		"user-1": "series_1",
		"user-2": "series_2",
	}

	ctx := context.Background()
	cfg, cleanup := prepareStorageConfig(t)
	defer cleanup()

	storageDir, err := ioutil.TempDir(os.TempDir(), "storage-*")
	require.NoError(t, err)

	for userID, metricName := range userToMetric {
		generateStorageBlock(t, storageDir, userID, metricName, 10, 100)
	}

	bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	reg := prometheus.NewPedanticRegistry()
	stores, err := NewBucketStores(cfg, nil, bucket, mockLoggingLevel(), log.NewNopLogger(), reg)
	require.NoError(t, err)

	// Query series before the initial sync.
	for userID, metricName := range userToMetric {
		seriesSet, warnings, err := querySeries(stores, userID, metricName, 20, 40)
		require.NoError(t, err)
		assert.Empty(t, warnings)
		assert.Empty(t, seriesSet)
	}

	require.NoError(t, stores.InitialSync(ctx))

	// Query series after the initial sync.
	for userID, metricName := range userToMetric {
		seriesSet, warnings, err := querySeries(stores, userID, metricName, 20, 40)
		require.NoError(t, err)
		assert.Empty(t, warnings)
		assert.Len(t, seriesSet, 1)
		assert.Equal(t, []storepb.Label{{Name: labels.MetricName, Value: metricName}}, seriesSet[0].Labels)
	}

	// Query series of another user.
	seriesSet, warnings, err := querySeries(stores, "user-1", "series_2", 20, 40)
	require.NoError(t, err)
	assert.Empty(t, warnings)
	assert.Empty(t, seriesSet)

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_querier_bucket_store_blocks_loaded Number of currently loaded blocks.
			# TYPE cortex_querier_bucket_store_blocks_loaded gauge
			cortex_querier_bucket_store_blocks_loaded 2

			# HELP cortex_querier_bucket_store_block_loads_total Total number of remote block loading attempts.
			# TYPE cortex_querier_bucket_store_block_loads_total counter
			cortex_querier_bucket_store_block_loads_total 2

			# HELP cortex_querier_bucket_store_block_load_failures_total Total number of failed remote block loading attempts.
			# TYPE cortex_querier_bucket_store_block_load_failures_total counter
			cortex_querier_bucket_store_block_load_failures_total 0
	`), "cortex_querier_bucket_store_blocks_loaded", "cortex_querier_bucket_store_block_loads_total", "cortex_querier_bucket_store_block_load_failures_total"))
}

func TestBucketStores_SyncBlocks(t *testing.T) {
	const (
		userID     = "user-1"
		metricName = "series_1"
	)

	ctx := context.Background()
	cfg, cleanup := prepareStorageConfig(t)
	defer cleanup()

	storageDir, err := ioutil.TempDir(os.TempDir(), "storage-*")
	require.NoError(t, err)

	bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	reg := prometheus.NewPedanticRegistry()
	stores, err := NewBucketStores(cfg, nil, bucket, mockLoggingLevel(), log.NewNopLogger(), reg)
	require.NoError(t, err)

	// Run an initial sync to discover 1 block.
	generateStorageBlock(t, storageDir, userID, metricName, 10, 100)
	require.NoError(t, stores.InitialSync(ctx))

	// Query a range for which we have no samples.
	seriesSet, warnings, err := querySeries(stores, userID, metricName, 150, 180)
	require.NoError(t, err)
	assert.Empty(t, warnings)
	assert.Empty(t, seriesSet)

	// Generate another block and sync blocks again.
	generateStorageBlock(t, storageDir, userID, metricName, 100, 200)
	require.NoError(t, stores.SyncBlocks(ctx))

	seriesSet, warnings, err = querySeries(stores, userID, metricName, 150, 180)
	require.NoError(t, err)
	assert.Empty(t, warnings)
	assert.Len(t, seriesSet, 1)
	assert.Equal(t, []storepb.Label{{Name: labels.MetricName, Value: metricName}}, seriesSet[0].Labels)

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_querier_bucket_store_blocks_loaded Number of currently loaded blocks.
			# TYPE cortex_querier_bucket_store_blocks_loaded gauge
			cortex_querier_bucket_store_blocks_loaded 2

			# HELP cortex_querier_bucket_store_block_loads_total Total number of remote block loading attempts.
			# TYPE cortex_querier_bucket_store_block_loads_total counter
			cortex_querier_bucket_store_block_loads_total 2

			# HELP cortex_querier_bucket_store_block_load_failures_total Total number of failed remote block loading attempts.
			# TYPE cortex_querier_bucket_store_block_load_failures_total counter
			cortex_querier_bucket_store_block_load_failures_total 0
	`), "cortex_querier_bucket_store_blocks_loaded", "cortex_querier_bucket_store_block_loads_total", "cortex_querier_bucket_store_block_load_failures_total"))
}

func prepareStorageConfig(t *testing.T) (cortex_tsdb.Config, func()) {
	tmpDir, err := ioutil.TempDir(os.TempDir(), "blocks-sync-*")
	require.NoError(t, err)

	cfg := cortex_tsdb.Config{}
	flagext.DefaultValues(&cfg)
	cfg.BucketStore.SyncDir = tmpDir

	cleanup := func() {
		require.NoError(t, os.RemoveAll(tmpDir))
	}

	return cfg, cleanup
}

func generateStorageBlock(t *testing.T, storageDir, userID string, metricName string, minT, maxT int64) {
	const step = 15

	// Create a directory for the user (if doesn't already exist).
	userDir := filepath.Join(storageDir, userID)
	if _, err := os.Stat(userDir); err != nil {
		require.NoError(t, os.Mkdir(userDir, os.ModePerm))
	}

	// Create a temporary directory where the TSDB is opened,
	// then it will be snapshotted to the storage directory.
	tmpDir, err := ioutil.TempDir(os.TempDir(), "tsdb-*")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(tmpDir))
	}()

	db, err := tsdb.Open(tmpDir, log.NewNopLogger(), nil, tsdb.DefaultOptions)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	series := labels.Labels{labels.Label{Name: labels.MetricName, Value: metricName}}

	app := db.Appender()
	for ts := minT; ts < maxT; ts += step {
		_, err = app.Add(series, ts, 1)
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())

	// Snapshot TSDB to the storage directory.
	require.NoError(t, db.Snapshot(userDir, true))
}

func querySeries(stores *BucketStores, userID, metricName string, minT, maxT int64) ([]*storepb.Series, storage.Warnings, error) {
	req := &storepb.SeriesRequest{
		MinTime: minT,
		MaxTime: maxT,
		Matchers: []storepb.LabelMatcher{{
			Type:  storepb.LabelMatcher_EQ,
			Name:  labels.MetricName,
			Value: metricName,
		}},
		PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
	}

	ctx := setUserIDToGRPCContext(context.Background(), userID)
	srv := newBucketStoreSeriesServer(ctx)
	err := stores.Series(req, srv)

	return srv.SeriesSet, srv.Warnings, err
}

func mockLoggingLevel() logging.Level {
	level := logging.Level{}
	err := level.Set("info")
	if err != nil {
		panic(err)
	}

	return level
}
