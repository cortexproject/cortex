package storegateway

import (
	"context"
	"errors"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
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
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
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

	// Create limits
	limits := validation.NewOverrides(validation.Limits{}, nil)

	// Create parquet bucket stores with caching
	parquetStores, err := newParquetBucketStores(storageCfg, bucketClient, limits, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NotNil(t, parquetStores)

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

func TestParquetBucketStores_Series_ShouldReturnErrorIfMaxInflightRequestIsReached(t *testing.T) {
	cfg := prepareStorageConfig(t)
	cfg.BucketStore.BucketStoreType = string(cortex_tsdb.ParquetBucketStore)
	cfg.BucketStore.MaxInflightRequests = 10
	reg := prometheus.NewPedanticRegistry()
	storageDir := t.TempDir()
	generateStorageBlock(t, storageDir, "user_id", "series_1", 0, 100, 15)
	bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	stores, err := NewBucketStores(cfg, NewNoShardingStrategy(log.NewNopLogger(), nil), objstore.WithNoopInstr(bucket), defaultLimitsOverrides(t), mockLoggingLevel(), log.NewNopLogger(), reg)
	require.NoError(t, err)
	require.NoError(t, stores.InitialSync(context.Background()))

	parquetStores := stores.(*ParquetBucketStores)
	// Set inflight requests to the limit
	for i := 0; i < 10; i++ {
		parquetStores.inflightRequests.Inc()
	}
	series, warnings, err := querySeries(stores, "user_id", "series_1", 0, 100)
	assert.ErrorIs(t, err, ErrTooManyInflightRequests)
	assert.Empty(t, series)
	assert.Empty(t, warnings)
}

//func TestParquetBucketStores_Series_ShouldNotCheckMaxInflightRequestsIfTheLimitIsDisabled(t *testing.T) {
//	cfg := prepareStorageConfig(t)
//	cfg.BucketStore.BucketStoreType = string(ParquetBucketStore)
//	reg := prometheus.NewPedanticRegistry()
//	storageDir := t.TempDir()
//	generateStorageBlock(t, storageDir, "user_id", "series_1", 0, 100, 15)
//	bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
//	require.NoError(t, err)
//
//	stores, err := NewBucketStores(cfg, NewNoShardingStrategy(log.NewNopLogger(), nil), objstore.WithNoopInstr(bucket), defaultLimitsOverrides(t), mockLoggingLevel(), log.NewNopLogger(), reg)
//	require.NoError(t, err)
//	require.NoError(t, stores.InitialSync(context.Background()))
//
//	parquetStores := stores.(*ParquetBucketStores)
//	// Set inflight requests to the limit (max_inflight_request is set to 0 by default = disabled)
//	for i := 0; i < 10; i++ {
//		parquetStores.inflightRequests.Inc()
//	}
//	series, _, err := querySeriesWithBlockIDs(stores, "user_id", "series_1", 0, 100)
//	require.NoError(t, err)
//	assert.Equal(t, 1, len(series))
//}
