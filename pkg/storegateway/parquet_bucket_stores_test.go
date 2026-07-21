package storegateway

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	ulidv2 "github.com/oklog/ulid/v2"
	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/bucket/filesystem"
	cortex_parquet "github.com/cortexproject/cortex/pkg/storage/parquet"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/cortexproject/cortex/pkg/util/users"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func TestParquetBucketStores_Series_NoUserID(t *testing.T) {
	stores := &ParquetBucketStores{
		logger: log.NewNopLogger(),
	}

	req := &storepb.SeriesRequest{
		MinTime: 0,
		MaxTime: 100,
		Matchers: []storepb.LabelMatcher{{
			Type:  storepb.LabelMatcher_EQ,
			Name:  labels.MetricName,
			Value: "test_metric",
		}},
	}

	srv := newBucketStoreSeriesServer(context.Background())
	err := stores.Series(req, srv)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no userID")
}

func TestParquetBucketStores_Series_StoreCreationError(t *testing.T) {
	// Create a mock bucket client
	mockBucket := &bucket.ClientMock{}

	stores := &ParquetBucketStores{
		logger:       log.NewNopLogger(),
		bucket:       mockBucket,
		stores:       make(map[string]*parquetBucketStore),
		storesErrors: make(map[string]error),
	}

	// Simulate a store creation error
	stores.storesErrors["user-1"] = errors.New("store creation failed")

	req := &storepb.SeriesRequest{
		MinTime: 0,
		MaxTime: 100,
		Matchers: []storepb.LabelMatcher{{
			Type:  storepb.LabelMatcher_EQ,
			Name:  labels.MetricName,
			Value: "test_metric",
		}},
	}

	ctx := setUserIDToGRPCContext(context.Background(), "user-1")
	srv := newBucketStoreSeriesServer(ctx)
	err := stores.Series(req, srv)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "store creation failed")
}

func TestParquetBucketStores_LabelNames_NoUserID(t *testing.T) {
	stores := &ParquetBucketStores{
		logger: log.NewNopLogger(),
	}

	req := &storepb.LabelNamesRequest{
		Start: 0,
		End:   100,
	}

	_, err := stores.LabelNames(context.Background(), req)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no userID")
}

func TestParquetBucketStores_LabelValues_NoUserID(t *testing.T) {
	stores := &ParquetBucketStores{
		logger: log.NewNopLogger(),
	}

	req := &storepb.LabelValuesRequest{
		Start: 0,
		End:   100,
		Label: "__name__",
	}

	_, err := stores.LabelValues(context.Background(), req)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no userID")
}

func TestParquetBucketStore_FindParquetBlocks_InvalidMatchers(t *testing.T) {
	store := &parquetBucketStore{
		logger: log.NewNopLogger(),
	}

	// Test with no matchers
	_, err := store.findParquetBlocks(context.Background(), nil)
	assert.Error(t, err)
	s, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, s.Code())

	// Test with multiple matchers
	_, err = store.findParquetBlocks(context.Background(), []storepb.LabelMatcher{
		{Type: storepb.LabelMatcher_RE, Name: block.BlockIDLabel, Value: "block1"},
		{Type: storepb.LabelMatcher_RE, Name: block.BlockIDLabel, Value: "block2"},
	})
	assert.Error(t, err)
	s, ok = status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, s.Code())

	// Test with wrong matcher type
	_, err = store.findParquetBlocks(context.Background(), []storepb.LabelMatcher{
		{Type: storepb.LabelMatcher_EQ, Name: block.BlockIDLabel, Value: "block1"},
	})
	assert.Error(t, err)
	s, ok = status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, s.Code())

	// Test with wrong matcher name
	_, err = store.findParquetBlocks(context.Background(), []storepb.LabelMatcher{
		{Type: storepb.LabelMatcher_RE, Name: "wrong_name", Value: "block1"},
	})
	assert.Error(t, err)
	s, ok = status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, s.Code())
}

func TestParquetBucketStore_ShardCountsFromIndex_Memoized(t *testing.T) {
	uid1 := ulidv2.MustNew(1, nil)
	uid2 := ulidv2.MustNew(2, nil)
	uid3 := ulidv2.MustNew(3, nil)
	idx := &bucketindex.Index{
		UpdatedAt: 100,
		Blocks: bucketindex.Blocks{
			{ID: uid1, Parquet: &cortex_parquet.ConverterMarkMeta{Shards: 3}},
			{ID: uid2}, // Non-parquet block -> excluded from the map.
			{ID: uid3, Parquet: &cortex_parquet.ConverterMarkMeta{Shards: 0}}, // Old parquet block without recorded shards -> 1.
		},
	}

	store := &parquetBucketStore{logger: log.NewNopLogger()}

	got := store.shardCountsFromIndex(idx)
	assert.Equal(t, 3, got[uid1.String()])
	_, ok := got[uid2.String()]
	assert.False(t, ok, "non-parquet blocks must not be tracked in the shard count map")
	assert.Equal(t, 1, got[uid3.String()], "parquet block without recorded shards defaults to 1")
	assert.Equal(t, int64(100), store.cachedIndexUpdatedAt)

	// Same UpdatedAt -> the map is reused rather than rebuilt.
	store.cachedShardCounts[uid1.String()] = 999
	reused := store.shardCountsFromIndex(idx)
	assert.Equal(t, 999, reused[uid1.String()], "map should be reused when UpdatedAt is unchanged")

	// A new UpdatedAt -> the map is rebuilt from the index.
	idx.UpdatedAt = 200
	rebuilt := store.shardCountsFromIndex(idx)
	assert.Equal(t, 3, rebuilt[uid1.String()], "map should be rebuilt when UpdatedAt changes")
	assert.Equal(t, int64(200), store.cachedIndexUpdatedAt)
}

func TestChunkToStoreEncoding(t *testing.T) {
	tests := []struct {
		name     string
		encoding chunkenc.Encoding
		expected storepb.Chunk_Encoding
	}{
		{
			name:     "XOR encoding",
			encoding: chunkenc.EncXOR,
			expected: storepb.Chunk_XOR,
		},
		{
			name:     "Histogram encoding",
			encoding: chunkenc.EncHistogram,
			expected: storepb.Chunk_HISTOGRAM,
		},
		{
			name:     "Float histogram encoding",
			encoding: chunkenc.EncFloatHistogram,
			expected: storepb.Chunk_FLOAT_HISTOGRAM,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := chunkToStoreEncoding(tt.encoding)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParquetBucketStoresWithCaching(t *testing.T) {
	// Create a temporary directory for the test
	tempDir := t.TempDir()

	// Create storage configuration with caching enabled
	storageCfg := cortex_tsdb.BlocksStorageConfig{
		UsersScanner: users.UsersScannerConfig{
			Strategy: users.UserScanStrategyList,
		},
		Bucket: bucket.Config{
			Backend: "filesystem",
			Filesystem: filesystem.Config{
				Directory: tempDir,
			},
		},
		BucketStore: cortex_tsdb.BucketStoreConfig{
			ChunksCache: cortex_tsdb.ChunksCacheConfig{
				BucketCacheBackend: cortex_tsdb.BucketCacheBackend{
					Backend: "inmemory",
				},
			},
			MetadataCache: cortex_tsdb.MetadataCacheConfig{
				BucketCacheBackend: cortex_tsdb.BucketCacheBackend{
					Backend: "inmemory",
				},
			},
			ParquetLabelsCache: cortex_tsdb.ParquetLabelsCacheConfig{
				BucketCacheBackend: cortex_tsdb.BucketCacheBackend{
					Backend: "inmemory",
				},
			},
			ParquetRowRangesCache: cortex_tsdb.ParquetRowRangesCacheConfig{
				BucketCacheBackend: cortex_tsdb.BucketCacheBackend{
					Backend: "inmemory",
				},
			},
		},
	}

	// Create a mock bucket client
	bucketClient, err := bucket.NewClient(context.Background(), storageCfg.Bucket, nil, "test", log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	// Create limits
	limits := validation.NewOverrides(validation.Limits{}, nil)

	// Create parquet bucket stores with caching
	parquetStores, err := newParquetBucketStores(storageCfg, bucketClient, nil, nil, limits, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NotNil(t, parquetStores)
	require.NotNil(t, parquetStores.rowRangesCache)

	store, err := parquetStores.getOrCreateStore("user-1")
	require.NoError(t, err)
	require.NotNil(t, store.rowRangesCache)
	require.Same(t, parquetStores.rowRangesCache, store.rowRangesCache)

	// Verify that the bucket is a caching bucket (it should be wrapped)
	// The caching bucket should be different from the original bucket client
	require.NotEqual(t, bucketClient, parquetStores.bucket)
}

func TestCreateCachingBucketClientForParquet(t *testing.T) {
	// Create a temporary directory for the test
	tempDir := t.TempDir()

	// Create storage configuration with caching enabled
	storageCfg := cortex_tsdb.BlocksStorageConfig{
		Bucket: bucket.Config{
			Backend: "filesystem",
			Filesystem: filesystem.Config{
				Directory: tempDir,
			},
		},
		BucketStore: cortex_tsdb.BucketStoreConfig{
			ChunksCache: cortex_tsdb.ChunksCacheConfig{
				BucketCacheBackend: cortex_tsdb.BucketCacheBackend{
					Backend: "inmemory",
				},
			},
			MetadataCache: cortex_tsdb.MetadataCacheConfig{
				BucketCacheBackend: cortex_tsdb.BucketCacheBackend{
					Backend: "inmemory",
				},
			},
			ParquetLabelsCache: cortex_tsdb.ParquetLabelsCacheConfig{
				BucketCacheBackend: cortex_tsdb.BucketCacheBackend{
					Backend: "inmemory",
				},
			},
		},
	}

	// Create a mock bucket client
	bucketClient, err := bucket.NewClient(context.Background(), storageCfg.Bucket, nil, "test", log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	// Create caching bucket client
	cachingBucket, err := createCachingBucketClientForParquet(storageCfg, bucketClient, "test", log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NotNil(t, cachingBucket)

	// Verify that the caching bucket is different from the original bucket client
	require.NotEqual(t, bucketClient, cachingBucket)
}

func convertToParquetBlocksForTesting(userPath string, userBkt objstore.InstrumentedBucket) ([]string, error) {
	return convertToParquetBlocksWithShardsForTesting(userPath, userBkt, 0, 0)
}

// convertToParquetBlocksWithShardsForTesting converts all TSDB blocks under userPath to parquet.
// numRowGroups and maxRowsPerRowGroup control how many shards are produced per block.
func convertToParquetBlocksWithShardsForTesting(userPath string, userBkt objstore.InstrumentedBucket, numRowGroups, maxRowsPerRowGroup int) ([]string, error) {
	var blockIDs []string

	pool := chunkenc.NewPool()
	userDir, err := os.ReadDir(userPath)
	if err != nil {
		return nil, err
	}

	for _, file := range userDir {
		uid, err := ulid.Parse(file.Name())
		if err != nil {
			continue
		}
		blockIDs = append(blockIDs, file.Name())
		bdir := filepath.Join(userPath, file.Name())

		tsdbBlock, err := tsdb.OpenBlock(nil, bdir, pool, tsdb.DefaultPostingsDecoderFactory)
		if err != nil {
			return nil, err
		}

		converterOptions := []convert.ConvertOption{convert.WithName(file.Name())}
		if numRowGroups > 0 {
			converterOptions = append(converterOptions, convert.WithNumRowGroups(numRowGroups))
		}
		if maxRowsPerRowGroup > 0 {
			converterOptions = append(converterOptions, convert.WithRowGroupSize(maxRowsPerRowGroup))
		}

		numShards, err := convert.ConvertTSDBBlock(context.Background(), userBkt, tsdbBlock.MinTime(), tsdbBlock.MaxTime(), []convert.Convertible{tsdbBlock}, promslog.NewNopLogger(), converterOptions...)
		_ = tsdbBlock.Close()
		if err != nil {
			return nil, err
		}

		// Write converter mark so findParquetBlocks knows the actual shard count.
		uidV2, err := ulidv2.Parse(uid.String())
		if err != nil {
			return nil, err
		}
		if err := cortex_parquet.WriteConverterMark(context.Background(), uidV2, userBkt, numShards); err != nil {
			return nil, err
		}
	}
	return blockIDs, nil
}

// generateStorageBlockWithMultipleSeries creates a TSDB block with numSeries unique series.
// Each series has labels {__name__=metricName, series=i}.
func generateStorageBlockWithMultipleSeries(t *testing.T, storageDir, userID, metricName string, numSeries int, minT, maxT int64, step int) {
	t.Helper()
	userDir := filepath.Join(storageDir, userID)
	if _, err := os.Stat(userDir); os.IsNotExist(err) {
		require.NoError(t, os.Mkdir(userDir, os.ModePerm))
	}

	tmpDir := t.TempDir()
	db, err := tsdb.Open(tmpDir, promslog.NewNopLogger(), nil, tsdb.DefaultOptions(), nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	app := db.Appender(context.Background())
	for i := range numSeries {
		lbls := labels.FromStrings(labels.MetricName, metricName, "series", strconv.Itoa(i))
		for ts := minT; ts < maxT; ts += int64(step) {
			_, err = app.Append(0, lbls, ts, float64(i))
			require.NoError(t, err)
		}
	}
	require.NoError(t, app.Commit())
	require.NoError(t, db.Snapshot(userDir, true))
}

// TestParquetBucketStores_Series_MultiShard verifies that when a parquet block is split into
// multiple shards, the Store Gateway reads all shards and returns the complete series set.
func TestParquetBucketStores_Series_MultiShard(t *testing.T) {
	const (
		userID     = "user-1"
		metricName = "test_metric"
		numSeries  = 6 // 6 unique series
		// numRowGroups=1, maxRowsPerRowGroup=2 → ceil(6/1*2) = 3 shards
		numRowGroups       = 1
		maxRowsPerRowGroup = 2
		expectedShards     = 3
	)

	cfg := prepareStorageConfig(t)
	cfg.BucketStore.BucketStoreType = string(cortex_tsdb.ParquetBucketStore)

	storageDir := t.TempDir()

	// Create a block with 6 unique series.
	generateStorageBlockWithMultipleSeries(t, storageDir, userID, metricName, numSeries, 0, 100, 15)

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	stores, err := NewBucketStores(cfg, NewNoShardingStrategy(log.NewNopLogger(), nil), objstore.WithNoopInstr(bkt), defaultLimitsOverrides(t), mockLoggingLevel(), log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	userPath := filepath.Join(storageDir, userID)
	overrides := validation.NewOverrides(validation.Limits{}, nil)
	uBucket := bucket.NewUserBucketClient(userID, bkt, overrides)

	// Convert to parquet with 3 shards and write converter mark.
	blockIDs, err := convertToParquetBlocksWithShardsForTesting(userPath, uBucket, numRowGroups, maxRowsPerRowGroup)
	require.NoError(t, err)
	require.Len(t, blockIDs, 1)

	// Verify converter mark shows 3 shards.
	uidV2, err := ulidv2.Parse(blockIDs[0])
	require.NoError(t, err)
	marker, err := cortex_parquet.ReadConverterMark(context.Background(), uidV2, uBucket, log.NewNopLogger())
	require.NoError(t, err)
	require.Equal(t, expectedShards, marker.Shards, "converter mark should record 3 shards")

	// Query and verify all 6 series are returned across all 3 shards.
	series, _, err := querySeries(stores, userID, metricName, 0, 100, blockIDs...)
	require.NoError(t, err)
	assert.Equal(t, numSeries, len(series), "all series from all shards must be returned")
}

// TestParquetBucketStores_Series_MultiShard_BucketIndex verifies that, when the bucket
// index is enabled, the Store Gateway resolves the parquet shard count from the bucket
// index instead of reading the per-block converter mark.
func TestParquetBucketStores_Series_MultiShard_BucketIndex(t *testing.T) {
	const (
		userID     = "user-1"
		metricName = "test_metric"
		numSeries  = 6 // 6 unique series
		// numRowGroups=1, maxRowsPerRowGroup=2 → ceil(6/1*2) = 3 shards
		numRowGroups       = 1
		maxRowsPerRowGroup = 2
		expectedShards     = 3
	)

	cfg := prepareStorageConfig(t)
	cfg.BucketStore.BucketStoreType = string(cortex_tsdb.ParquetBucketStore)
	cfg.BucketStore.BucketIndex.Enabled = true

	storageDir := t.TempDir()

	// Create a block with 6 unique series.
	generateStorageBlockWithMultipleSeries(t, storageDir, userID, metricName, numSeries, 0, 100, 15)

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	overrides := validation.NewOverrides(validation.Limits{}, nil)
	uBucket := bucket.NewUserBucketClient(userID, bkt, overrides)

	// Convert to parquet with 3 shards and write the (correct) converter mark.
	userPath := filepath.Join(storageDir, userID)
	blockIDs, err := convertToParquetBlocksWithShardsForTesting(userPath, uBucket, numRowGroups, maxRowsPerRowGroup)
	require.NoError(t, err)
	require.Len(t, blockIDs, 1)

	uidV2, err := ulidv2.Parse(blockIDs[0])
	require.NoError(t, err)

	// The bucket index discovers parquet blocks via the global markers location
	// (parquet-markers/<blockID>-parquet-converter-mark.json), so upload it there too.
	require.NoError(t, uBucket.Upload(context.Background(), bucketindex.ConverterMarkFilePath(uidV2), bytes.NewReader([]byte("{}"))))

	// Build the bucket index (with parquet info) so the shard count is recorded there.
	idx, _, _, err := bucketindex.NewUpdater(bkt, userID, nil, log.NewNopLogger()).EnableParquet().UpdateIndex(context.Background(), nil)
	require.NoError(t, err)
	require.NoError(t, bucketindex.WriteIndex(context.Background(), bkt, userID, nil, idx))

	// The bucket index must record the actual number of shards (3).
	require.Len(t, idx.Blocks, 1)
	require.NotNil(t, idx.Blocks[0].Parquet)
	require.Equal(t, expectedShards, idx.Blocks[0].Parquet.Shards, "bucket index should record 3 shards")

	// Overwrite the converter mark with a wrong shard count (1). If the Store Gateway
	// used the converter mark instead of the bucket index, it would only read 1 shard.
	require.NoError(t, cortex_parquet.WriteConverterMark(context.Background(), uidV2, uBucket, 1))

	stores, err := NewBucketStores(cfg, NewNoShardingStrategy(log.NewNopLogger(), nil), objstore.WithNoopInstr(bkt), defaultLimitsOverrides(t), mockLoggingLevel(), log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	series, _, err := querySeries(stores, userID, metricName, 0, 100, blockIDs...)
	require.NoError(t, err)
	assert.Equal(t, numSeries, len(series), "all series must be returned using the bucket index shard count")
}

// TestParquetBucketStores_Series_MultiShard_BucketIndexStale_FallbackToConverterMark
// verifies that, when the bucket index is enabled but doesn't yet carry the parquet shard
// info for a block, the Store Gateway falls back to reading the converter mark instead of
// defaulting to a single shard.
func TestParquetBucketStores_Series_MultiShard_BucketIndexStale_FallbackToConverterMark(t *testing.T) {
	const (
		userID     = "user-1"
		metricName = "test_metric"
		numSeries  = 6 // 6 unique series
		// numRowGroups=1, maxRowsPerRowGroup=2 → ceil(6/1*2) = 3 shards
		numRowGroups       = 1
		maxRowsPerRowGroup = 2
	)

	cfg := prepareStorageConfig(t)
	cfg.BucketStore.BucketStoreType = string(cortex_tsdb.ParquetBucketStore)
	cfg.BucketStore.BucketIndex.Enabled = true

	storageDir := t.TempDir()

	generateStorageBlockWithMultipleSeries(t, storageDir, userID, metricName, numSeries, 0, 100, 15)

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	overrides := validation.NewOverrides(validation.Limits{}, nil)
	uBucket := bucket.NewUserBucketClient(userID, bkt, overrides)

	// Build the bucket index before converting to parquet, without parquet info. This models
	// a stale index that knows about the block but doesn't record its shard count yet.
	idx, _, _, err := bucketindex.NewUpdater(bkt, userID, nil, log.NewNopLogger()).UpdateIndex(context.Background(), nil)
	require.NoError(t, err)
	require.NoError(t, bucketindex.WriteIndex(context.Background(), bkt, userID, nil, idx))
	require.Len(t, idx.Blocks, 1)
	require.Nil(t, idx.Blocks[0].Parquet, "bucket index must not carry parquet shard info yet")

	// Convert to parquet with 3 shards and write the correct converter mark.
	userPath := filepath.Join(storageDir, userID)
	blockIDs, err := convertToParquetBlocksWithShardsForTesting(userPath, uBucket, numRowGroups, maxRowsPerRowGroup)
	require.NoError(t, err)
	require.Len(t, blockIDs, 1)

	stores, err := newParquetBucketStores(cfg, objstore.WithNoopInstr(bkt), nil, nil, overrides, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	series, _, err := querySeries(stores, userID, metricName, 0, 100, blockIDs...)
	require.NoError(t, err)
	assert.Equal(t, numSeries, len(series), "all series from all shards must be returned via the converter mark fallback")
}
