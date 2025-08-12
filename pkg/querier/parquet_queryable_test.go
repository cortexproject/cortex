package querier

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/querier/series"
	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/cortexproject/cortex/pkg/storage/bucket/filesystem"
	"github.com/cortexproject/cortex/pkg/storage/parquet"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	cortex_testutil "github.com/cortexproject/cortex/pkg/storage/tsdb/testutil"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/limiter"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func TestParquetQueryableFallbackLogic(t *testing.T) {
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	minT := int64(10)
	maxT := util.TimeToMillis(time.Now())

	createStore := func() *blocksStoreSetMock {
		return &blocksStoreSetMock{mockedResponses: []interface{}{
			map[BlocksStoreClient][]ulid.ULID{
				&storeGatewayClientMock{remoteAddr: "1.1.1.1",
					mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.FromStrings(labels.MetricName, "fromSg"), []cortexpb.Sample{{Value: 1, TimestampMs: minT}, {Value: 2, TimestampMs: minT + 1}}, nil, nil),
						mockHintsResponse(block1, block2),
					},
					mockedLabelNamesResponse: &storepb.LabelNamesResponse{
						Names:    namesFromSeries(labels.FromMap(map[string]string{labels.MetricName: "fromSg", "fromSg": "fromSg"})),
						Warnings: []string{},
						Hints:    mockNamesHints(block1, block2),
					},
					mockedLabelValuesResponse: &storepb.LabelValuesResponse{
						Values:   valuesFromSeries(labels.MetricName, labels.FromMap(map[string]string{labels.MetricName: "fromSg", "fromSg": "fromSg"})),
						Warnings: []string{},
						Hints:    mockValuesHints(block1, block2),
					},
				}: {block1, block2}},
		},
		}
	}

	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "fromSg"),
	}
	ctx := user.InjectOrgID(context.Background(), "user-1")

	t.Run("should fallback all blocks", func(t *testing.T) {
		finder := &blocksFinderMock{}
		stores := createStore()

		q := &blocksStoreQuerier{
			minT:        minT,
			maxT:        maxT,
			finder:      finder,
			stores:      stores,
			consistency: NewBlocksConsistencyChecker(0, 0, log.NewNopLogger(), nil),
			logger:      log.NewNopLogger(),
			metrics:     newBlocksStoreQueryableMetrics(prometheus.NewPedanticRegistry()),
			limits:      &blocksStoreLimitsMock{},

			storeGatewayConsistencyCheckMaxAttempts: 3,
		}

		mParquetQuerier := &mockParquetQuerier{}
		pq := &parquetQuerierWithFallback{
			minT:                  minT,
			maxT:                  maxT,
			finder:                finder,
			blocksStoreQuerier:    q,
			parquetQuerier:        mParquetQuerier,
			queryStoreAfter:       time.Hour,
			metrics:               newParquetQueryableFallbackMetrics(prometheus.NewRegistry()),
			limits:                defaultOverrides(t, 0),
			logger:                log.NewNopLogger(),
			defaultBlockStoreType: parquetBlockStore,
		}

		finder.On("GetBlocks", mock.Anything, "user-1", minT, mock.Anything, mock.Anything).Return(bucketindex.Blocks{
			&bucketindex.Block{ID: block1},
			&bucketindex.Block{ID: block2},
		}, map[ulid.ULID]*bucketindex.BlockDeletionMark(nil), nil)

		t.Run("select", func(t *testing.T) {
			ss := pq.Select(ctx, true, nil, matchers...)
			require.NoError(t, ss.Err())
			require.Len(t, stores.queriedBlocks, 2)
			require.Len(t, mParquetQuerier.queriedBlocks, 0)
		})

		t.Run("labelNames", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			_, _, err := pq.LabelNames(ctx, nil, matchers...)
			require.NoError(t, err)
			require.Len(t, stores.queriedBlocks, 2)
			require.Len(t, mParquetQuerier.queriedBlocks, 0)
		})

		t.Run("labelValues", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			_, _, err := pq.LabelValues(ctx, labels.MetricName, nil, matchers...)
			require.NoError(t, err)
			require.Len(t, stores.queriedBlocks, 2)
			require.Len(t, mParquetQuerier.queriedBlocks, 0)
		})
	})

	t.Run("should fallback partial blocks", func(t *testing.T) {
		finder := &blocksFinderMock{}
		stores := createStore()

		q := &blocksStoreQuerier{
			minT:        minT,
			maxT:        maxT,
			finder:      finder,
			stores:      stores,
			consistency: NewBlocksConsistencyChecker(0, 0, log.NewNopLogger(), nil),
			logger:      log.NewNopLogger(),
			metrics:     newBlocksStoreQueryableMetrics(prometheus.NewPedanticRegistry()),
			limits:      &blocksStoreLimitsMock{},

			storeGatewayConsistencyCheckMaxAttempts: 3,
		}

		mParquetQuerier := &mockParquetQuerier{}
		pq := &parquetQuerierWithFallback{
			minT:                  minT,
			maxT:                  maxT,
			finder:                finder,
			blocksStoreQuerier:    q,
			parquetQuerier:        mParquetQuerier,
			metrics:               newParquetQueryableFallbackMetrics(prometheus.NewRegistry()),
			limits:                defaultOverrides(t, 0),
			logger:                log.NewNopLogger(),
			defaultBlockStoreType: parquetBlockStore,
		}

		finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT, mock.Anything).Return(bucketindex.Blocks{
			&bucketindex.Block{ID: block1, Parquet: &parquet.ConverterMarkMeta{Version: 1}},
			&bucketindex.Block{ID: block2},
		}, map[ulid.ULID]*bucketindex.BlockDeletionMark(nil), nil)

		t.Run("select", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			ss := pq.Select(ctx, true, nil, matchers...)
			require.NoError(t, ss.Err())
			require.Len(t, stores.queriedBlocks, 1)
			require.Len(t, mParquetQuerier.queriedBlocks, 1)
		})

		t.Run("labelNames", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			r, _, err := pq.LabelNames(ctx, nil, matchers...)
			require.NoError(t, err)
			require.Len(t, stores.queriedBlocks, 1)
			require.Len(t, mParquetQuerier.queriedBlocks, 1)
			require.Contains(t, r, "fromSg")
			require.Contains(t, r, "fromParquet")
		})

		t.Run("labelValues", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			r, _, err := pq.LabelValues(ctx, labels.MetricName, nil, matchers...)
			require.NoError(t, err)
			require.Len(t, stores.queriedBlocks, 1)
			require.Len(t, mParquetQuerier.queriedBlocks, 1)
			require.Contains(t, r, "fromSg")
			require.Contains(t, r, "fromParquet")
		})
	})

	t.Run("should query only parquet blocks when possible", func(t *testing.T) {
		finder := &blocksFinderMock{}
		stores := createStore()

		q := &blocksStoreQuerier{
			minT:        minT,
			maxT:        maxT,
			finder:      finder,
			stores:      stores,
			consistency: NewBlocksConsistencyChecker(0, 0, log.NewNopLogger(), nil),
			logger:      log.NewNopLogger(),
			metrics:     newBlocksStoreQueryableMetrics(prometheus.NewPedanticRegistry()),
			limits:      &blocksStoreLimitsMock{},

			storeGatewayConsistencyCheckMaxAttempts: 3,
		}

		mParquetQuerier := &mockParquetQuerier{}
		queryStoreAfter := time.Hour
		pq := &parquetQuerierWithFallback{
			minT:                  minT,
			maxT:                  maxT,
			finder:                finder,
			blocksStoreQuerier:    q,
			parquetQuerier:        mParquetQuerier,
			queryStoreAfter:       queryStoreAfter,
			metrics:               newParquetQueryableFallbackMetrics(prometheus.NewRegistry()),
			limits:                defaultOverrides(t, 0),
			logger:                log.NewNopLogger(),
			defaultBlockStoreType: parquetBlockStore,
		}

		finder.On("GetBlocks", mock.Anything, "user-1", minT, mock.Anything, mock.Anything).Return(bucketindex.Blocks{
			&bucketindex.Block{ID: block1, Parquet: &parquet.ConverterMarkMeta{Version: 1}},
			&bucketindex.Block{ID: block2, Parquet: &parquet.ConverterMarkMeta{Version: 1}},
		}, map[ulid.ULID]*bucketindex.BlockDeletionMark(nil), nil)

		t.Run("select", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			hints := storage.SelectHints{
				Start: minT,
				End:   maxT,
			}
			ss := pq.Select(ctx, true, &hints, matchers...)
			require.NoError(t, ss.Err())
			require.Len(t, stores.queriedBlocks, 0)
			require.Len(t, mParquetQuerier.queriedBlocks, 2)
			require.Equal(t, mParquetQuerier.queriedHints.Start, minT)
			queriedDelta := time.Duration(maxT-mParquetQuerier.queriedHints.End) * time.Millisecond
			require.InDeltaf(t, queriedDelta.Minutes(), queryStoreAfter.Minutes(), 0.1, "query after not set")
		})

		t.Run("labelNames", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			r, _, err := pq.LabelNames(ctx, nil, matchers...)
			require.NoError(t, err)
			require.Len(t, stores.queriedBlocks, 0)
			require.Len(t, mParquetQuerier.queriedBlocks, 2)
			require.NotContains(t, r, "fromSg")
			require.Contains(t, r, "fromParquet")
		})

		t.Run("labelValues", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			r, _, err := pq.LabelValues(ctx, labels.MetricName, nil, matchers...)
			require.NoError(t, err)
			require.Len(t, stores.queriedBlocks, 0)
			require.Len(t, mParquetQuerier.queriedBlocks, 2)
			require.NotContains(t, r, "fromSg")
			require.Contains(t, r, "fromParquet")
		})
	})

	t.Run("Default query TSDB block store even if parquet blocks available. Override with ctx", func(t *testing.T) {
		finder := &blocksFinderMock{}
		stores := createStore()

		q := &blocksStoreQuerier{
			minT:        minT,
			maxT:        maxT,
			finder:      finder,
			stores:      stores,
			consistency: NewBlocksConsistencyChecker(0, 0, log.NewNopLogger(), nil),
			logger:      log.NewNopLogger(),
			metrics:     newBlocksStoreQueryableMetrics(prometheus.NewPedanticRegistry()),
			limits:      &blocksStoreLimitsMock{},

			storeGatewayConsistencyCheckMaxAttempts: 3,
		}

		mParquetQuerier := &mockParquetQuerier{}
		pq := &parquetQuerierWithFallback{
			minT:                  minT,
			maxT:                  maxT,
			finder:                finder,
			blocksStoreQuerier:    q,
			parquetQuerier:        mParquetQuerier,
			metrics:               newParquetQueryableFallbackMetrics(prometheus.NewRegistry()),
			limits:                defaultOverrides(t, 0),
			logger:                log.NewNopLogger(),
			defaultBlockStoreType: tsdbBlockStore,
		}

		finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT, mock.Anything).Return(bucketindex.Blocks{
			&bucketindex.Block{ID: block1, Parquet: &parquet.ConverterMarkMeta{Version: 1}},
			&bucketindex.Block{ID: block2, Parquet: &parquet.ConverterMarkMeta{Version: 1}},
		}, map[ulid.ULID]*bucketindex.BlockDeletionMark(nil), nil)

		t.Run("select", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			ss := pq.Select(ctx, true, nil, matchers...)
			require.NoError(t, ss.Err())
			require.Len(t, stores.queriedBlocks, 2)
			require.Len(t, mParquetQuerier.queriedBlocks, 0)
		})

		t.Run("select with ctx key override to parquet", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			newCtx := AddBlockStoreTypeToContext(ctx, string(parquetBlockStore))
			ss := pq.Select(newCtx, true, nil, matchers...)
			require.NoError(t, ss.Err())
			require.Len(t, stores.queriedBlocks, 0)
			require.Len(t, mParquetQuerier.queriedBlocks, 2)
		})

		t.Run("labelNames", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			r, _, err := pq.LabelNames(ctx, nil, matchers...)
			require.NoError(t, err)
			require.Len(t, stores.queriedBlocks, 2)
			require.Len(t, mParquetQuerier.queriedBlocks, 0)
			require.Contains(t, r, "fromSg")
			require.NotContains(t, r, "fromParquet")
		})

		t.Run("labelNames with ctx key override to parquet", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			newCtx := AddBlockStoreTypeToContext(ctx, string(parquetBlockStore))
			r, _, err := pq.LabelNames(newCtx, nil, matchers...)
			require.NoError(t, err)
			require.Len(t, stores.queriedBlocks, 0)
			require.Len(t, mParquetQuerier.queriedBlocks, 2)
			require.NotContains(t, r, "fromSg")
			require.Contains(t, r, "fromParquet")
		})

		t.Run("labelValues", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			r, _, err := pq.LabelValues(ctx, labels.MetricName, nil, matchers...)
			require.NoError(t, err)
			require.Len(t, stores.queriedBlocks, 2)
			require.Len(t, mParquetQuerier.queriedBlocks, 0)
			require.Contains(t, r, "fromSg")
			require.NotContains(t, r, "fromParquet")
		})

		t.Run("labelValues with ctx key override to parquet", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			newCtx := AddBlockStoreTypeToContext(ctx, string(parquetBlockStore))
			r, _, err := pq.LabelValues(newCtx, labels.MetricName, nil, matchers...)
			require.NoError(t, err)
			require.Len(t, stores.queriedBlocks, 0)
			require.Len(t, mParquetQuerier.queriedBlocks, 2)
			require.NotContains(t, r, "fromSg")
			require.Contains(t, r, "fromParquet")
		})
	})
}

func TestParquetQueryable_Limits(t *testing.T) {
	t.Parallel()

	const (
		metricName = "test_metric"
		minT       = int64(0)
		maxT       = int64(1000)
	)

	bkt, tempDir := cortex_testutil.PrepareFilesystemBucket(t)

	config := Config{
		QueryStoreAfter:                         0,
		StoreGatewayQueryStatsEnabled:           false,
		StoreGatewayConsistencyCheckMaxAttempts: 3,
		ParquetQueryableShardCacheSize:          100,
		ParquetQueryableDefaultBlockStore:       "parquet",
	}

	storageCfg := cortex_tsdb.BlocksStorageConfig{
		Bucket: bucket.Config{
			Backend: "filesystem",
			Filesystem: filesystem.Config{
				Directory: tempDir,
			},
		},
	}

	ctx := context.Background()
	seriesCount := 100
	lbls := make([]labels.Labels, seriesCount)
	for i := 0; i < seriesCount; i++ {
		lbls[i] = labels.FromStrings(labels.MetricName, metricName, "series", strconv.Itoa(i))
	}

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	blockID, err := e2e.CreateBlock(ctx, rnd, tempDir, lbls, 100, 0, 1000, 10, 1000)
	require.NoError(t, err)

	blockDir := filepath.Join(tempDir, blockID.String())
	userBkt := bucket.NewUserBucketClient("user-1", bkt, nil)
	err = block.Upload(ctx, log.NewNopLogger(), userBkt, blockDir, metadata.NoneFunc)
	require.NoError(t, err)

	err = convertBlockToParquet(t, ctx, userBkt, blockID, blockDir)
	require.NoError(t, err)

	// Create a mocked bucket index blocks finder
	finder := &blocksFinderMock{}
	finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT, mock.Anything).Return(bucketindex.Blocks{
		&bucketindex.Block{ID: blockID, Parquet: &parquet.ConverterMarkMeta{Version: parquet.CurrentVersion}},
	}, map[ulid.ULID]*bucketindex.BlockDeletionMark(nil), nil)

	tests := map[string]struct {
		limits       *validation.Overrides
		queryLimiter *limiter.QueryLimiter
		expectedErr  error
	}{
		"row count limit hit - Parquet Queryable": {
			limits: func() *validation.Overrides {
				limits := validation.Limits{}
				flagext.DefaultValues(&limits)
				limits.ParquetMaxFetchedRowCount = 1
				return validation.NewOverrides(limits, nil)
			}(),
			queryLimiter: limiter.NewQueryLimiter(0, 0, 0, 0),
			expectedErr:  fmt.Errorf("would fetch too many rows: resource exhausted (used 1)"),
		},
		"max series per query limit hit": {
			limits: func() *validation.Overrides {
				limits := validation.Limits{}
				flagext.DefaultValues(&limits)
				return validation.NewOverrides(limits, nil)
			}(),
			queryLimiter: limiter.NewQueryLimiter(1, 0, 0, 0),
			expectedErr:  validation.LimitError(fmt.Sprintf(limiter.ErrMaxSeriesHit, 1)),
		},
		"max chunks per query limit hit": {
			limits: func() *validation.Overrides {
				limits := validation.Limits{}
				flagext.DefaultValues(&limits)
				return validation.NewOverrides(limits, nil)
			}(),
			queryLimiter: limiter.NewQueryLimiter(0, 0, 1, 0),
			expectedErr:  validation.LimitError(fmt.Sprintf(limiter.ErrMaxChunksPerQueryLimit, 1)),
		},
		"max chunk page size limit hit - Parquet Queryable": {
			limits: func() *validation.Overrides {
				limits := validation.Limits{}
				flagext.DefaultValues(&limits)
				limits.ParquetMaxFetchedChunkBytes = 1
				return validation.NewOverrides(limits, nil)
			}(),
			queryLimiter: limiter.NewQueryLimiter(0, 1, 0, 0),
			expectedErr:  fmt.Errorf("materializer failed to materialize chunks: would fetch too many chunk bytes: resource exhausted (used 1)"),
		},
		"max chunk bytes per query limit hit": {
			limits: func() *validation.Overrides {
				limits := validation.Limits{}
				flagext.DefaultValues(&limits)
				return validation.NewOverrides(limits, nil)
			}(),
			queryLimiter: limiter.NewQueryLimiter(0, 1, 0, 0),
			expectedErr:  validation.LimitError(fmt.Sprintf(limiter.ErrMaxChunkBytesHit, 1)),
		},
		"max data bytes per query limit hit": {
			limits: func() *validation.Overrides {
				limits := validation.Limits{}
				flagext.DefaultValues(&limits)
				limits.ParquetMaxFetchedDataBytes = 1
				return validation.NewOverrides(limits, nil)
			}(),
			queryLimiter: limiter.NewQueryLimiter(0, 0, 0, 1),
			expectedErr:  fmt.Errorf("error materializing labels: failed to get column indexes: failed to materialize column indexes: would fetch too many data bytes: resource exhausted (used 1)"),
		},
		"limits within bounds - should succeed": {
			limits: func() *validation.Overrides {
				limits := validation.Limits{}
				flagext.DefaultValues(&limits)
				limits.MaxFetchedSeriesPerQuery = 1000
				limits.MaxFetchedChunkBytesPerQuery = 1000000
				limits.MaxFetchedDataBytesPerQuery = 1000000
				return validation.NewOverrides(limits, nil)
			}(),
			queryLimiter: limiter.NewQueryLimiter(1000, 1000000, 1000, 1000000),
			expectedErr:  nil,
		},
	}

	for testName, testData := range tests {
		testData := testData
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			ctx := user.InjectOrgID(context.Background(), "user-1")
			ctx = limiter.AddQueryLimiterToContext(ctx, testData.queryLimiter)

			mockBlocksStoreQueryable := &BlocksStoreQueryable{finder: finder, Service: services.NewIdleService(func(_ context.Context) error {
				return nil
			}, func(_ error) error {
				return nil
			})}

			parquetQueryable, err := NewParquetQueryable(config, storageCfg, testData.limits, mockBlocksStoreQueryable, log.NewNopLogger(), prometheus.NewRegistry())
			require.NoError(t, err)
			err = services.StartAndAwaitRunning(ctx, parquetQueryable.(*parquetQueryableWithFallback))
			require.NoError(t, err)

			querier, err := parquetQueryable.Querier(minT, maxT)
			require.NoError(t, err)
			defer querier.Close()

			matchers := []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metricName),
			}

			set := querier.Select(ctx, true, nil, matchers...)
			if testData.expectedErr != nil {
				require.False(t, set.Next())
				err = set.Err()
				require.EqualError(t, err, testData.expectedErr.Error())
				return
			}

			require.NoError(t, set.Err())
		})
	}
}

// convertBlockToParquet converts a TSDB block to parquet and uploads it to the bucket
func convertBlockToParquet(t *testing.T, ctx context.Context, userBucketClient objstore.Bucket, blockID ulid.ULID, blockDir string) error {
	tsdbBlock, err := tsdb.OpenBlock(nil, blockDir, chunkenc.NewPool(), tsdb.DefaultPostingsDecoderFactory)
	require.NoError(t, err)

	converterOpts := []convert.ConvertOption{
		convert.WithSortBy(labels.MetricName),
		convert.WithColDuration(time.Hour * 8),
		convert.WithRowGroupSize(1000),
		convert.WithName(blockID.String()),
	}

	_, err = convert.ConvertTSDBBlock(
		ctx,
		userBucketClient,
		tsdbBlock.MinTime(),
		tsdbBlock.MaxTime(),
		[]convert.Convertible{tsdbBlock},
		converterOpts...,
	)
	require.NoError(t, err)

	_ = tsdbBlock.Close()

	// Write parquet converter marker
	err = parquet.WriteConverterMark(ctx, blockID, userBucketClient)
	require.NoError(t, err)

	return nil
}

func defaultOverrides(t *testing.T, queryVerticalShardSize int) *validation.Overrides {
	limits := validation.Limits{}
	flagext.DefaultValues(&limits)
	limits.QueryVerticalShardSize = queryVerticalShardSize

	overrides := validation.NewOverrides(limits, nil)
	return overrides
}

type mockParquetQuerier struct {
	queriedBlocks []*bucketindex.Block
	queriedHints  *storage.SelectHints
}

func (m *mockParquetQuerier) Select(ctx context.Context, sortSeries bool, sp *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	if blocks, ok := ExtractBlocksFromContext(ctx); ok {
		m.queriedBlocks = append(m.queriedBlocks, blocks...)
	}
	m.queriedHints = sp
	return series.NewConcreteSeriesSet(sortSeries, nil)
}

func (m *mockParquetQuerier) LabelValues(ctx context.Context, name string, _ *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	if blocks, ok := ExtractBlocksFromContext(ctx); ok {
		m.queriedBlocks = append(m.queriedBlocks, blocks...)
	}
	return []string{"fromParquet"}, nil, nil
}

func (m *mockParquetQuerier) LabelNames(ctx context.Context, _ *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	if blocks, ok := ExtractBlocksFromContext(ctx); ok {
		m.queriedBlocks = append(m.queriedBlocks, blocks...)
	}
	return []string{"fromParquet"}, nil, nil
}

func (m *mockParquetQuerier) Reset() {
	m.queriedBlocks = nil
}

func (mockParquetQuerier) Close() error {
	return nil
}

func TestMaterializedLabelsFilterCallback(t *testing.T) {
	tests := []struct {
		name                     string
		setupContext             func() context.Context
		expectedFilterReturned   bool
		expectedCallbackReturned bool
	}{
		{
			name: "no shard matcher in context",
			setupContext: func() context.Context {
				return context.Background()
			},
			expectedFilterReturned:   false,
			expectedCallbackReturned: false,
		},
		{
			name: "shard matcher exists but is not sharded",
			setupContext: func() context.Context {
				// Create a ShardInfo with TotalShards = 0 (not sharded)
				shardInfo := &storepb.ShardInfo{
					ShardIndex:  0,
					TotalShards: 0, // Not sharded
					By:          true,
					Labels:      []string{"__name__"},
				}

				buffers := &sync.Pool{New: func() interface{} {
					b := make([]byte, 0, 100)
					return &b
				}}
				shardMatcher := shardInfo.Matcher(buffers)

				return injectShardMatcherIntoContext(context.Background(), shardMatcher)
			},
			expectedFilterReturned:   false,
			expectedCallbackReturned: false,
		},
		{
			name: "shard matcher exists and is sharded",
			setupContext: func() context.Context {
				// Create a ShardInfo with TotalShards > 0 (sharded)
				shardInfo := &storepb.ShardInfo{
					ShardIndex:  0,
					TotalShards: 2, // Sharded
					By:          true,
					Labels:      []string{"__name__"},
				}

				buffers := &sync.Pool{New: func() interface{} {
					b := make([]byte, 0, 100)
					return &b
				}}
				shardMatcher := shardInfo.Matcher(buffers)

				return injectShardMatcherIntoContext(context.Background(), shardMatcher)
			},
			expectedFilterReturned:   true,
			expectedCallbackReturned: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.setupContext()

			filter, exists := materializedLabelsFilterCallback(ctx, nil)

			require.Equal(t, tt.expectedCallbackReturned, exists)

			if tt.expectedFilterReturned {
				require.NotNil(t, filter)

				// Test that the filter can be used
				testLabels := labels.FromStrings("__name__", "test_metric", "label1", "value1")
				// We can't easily test the actual filtering logic without knowing the internal
				// shard matching implementation, but we can at least verify the filter interface works
				_ = filter.Filter(testLabels)

				// Cleanup
				filter.Close()
			} else {
				require.Nil(t, filter)
			}
		})
	}
}

func TestParquetQueryableFallbackDisabled(t *testing.T) {
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	minT := int64(10)
	maxT := util.TimeToMillis(time.Now())

	createStore := func() *blocksStoreSetMock {
		return &blocksStoreSetMock{mockedResponses: []interface{}{
			map[BlocksStoreClient][]ulid.ULID{
				&storeGatewayClientMock{remoteAddr: "1.1.1.1",
					mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.FromStrings(labels.MetricName, "fromSg"), []cortexpb.Sample{{Value: 1, TimestampMs: minT}, {Value: 2, TimestampMs: minT + 1}}, nil, nil),
						mockHintsResponse(block1, block2),
					},
					mockedLabelNamesResponse: &storepb.LabelNamesResponse{
						Names:    namesFromSeries(labels.FromMap(map[string]string{labels.MetricName: "fromSg", "fromSg": "fromSg"})),
						Warnings: []string{},
						Hints:    mockNamesHints(block1, block2),
					},
					mockedLabelValuesResponse: &storepb.LabelValuesResponse{
						Values:   valuesFromSeries(labels.MetricName, labels.FromMap(map[string]string{labels.MetricName: "fromSg", "fromSg": "fromSg"})),
						Warnings: []string{},
						Hints:    mockValuesHints(block1, block2),
					},
				}: {block1, block2}},
		},
		}
	}

	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "fromSg"),
	}
	ctx := user.InjectOrgID(context.Background(), "user-1")

	t.Run("should return consistency check errors when fallback disabled and some blocks not available as parquet", func(t *testing.T) {
		finder := &blocksFinderMock{}
		stores := createStore()

		q := &blocksStoreQuerier{
			minT:        minT,
			maxT:        maxT,
			finder:      finder,
			stores:      stores,
			consistency: NewBlocksConsistencyChecker(0, 0, log.NewNopLogger(), nil),
			logger:      log.NewNopLogger(),
			metrics:     newBlocksStoreQueryableMetrics(prometheus.NewPedanticRegistry()),
			limits:      &blocksStoreLimitsMock{},

			storeGatewayConsistencyCheckMaxAttempts: 3,
		}

		mParquetQuerier := &mockParquetQuerier{}
		pq := &parquetQuerierWithFallback{
			minT:                  minT,
			maxT:                  maxT,
			finder:                finder,
			blocksStoreQuerier:    q,
			parquetQuerier:        mParquetQuerier,
			queryStoreAfter:       time.Hour,
			metrics:               newParquetQueryableFallbackMetrics(prometheus.NewRegistry()),
			limits:                defaultOverrides(t, 0),
			logger:                log.NewNopLogger(),
			defaultBlockStoreType: parquetBlockStore,
			fallbackDisabled:      true, // Disable fallback
		}

		// Set up blocks where block1 has parquet metadata but block2 doesn't
		finder.On("GetBlocks", mock.Anything, "user-1", minT, mock.Anything, mock.Anything).Return(bucketindex.Blocks{
			&bucketindex.Block{ID: block1, Parquet: &parquet.ConverterMarkMeta{Version: 1}}, // Available as parquet
			&bucketindex.Block{ID: block2}, // Not available as parquet
		}, map[ulid.ULID]*bucketindex.BlockDeletionMark(nil), nil)

		expectedError := fmt.Sprintf("consistency check failed because some blocks were not available as parquet files: %s", block2.String())

		t.Run("select should return consistency check error", func(t *testing.T) {
			ss := pq.Select(ctx, true, nil, matchers...)
			require.Error(t, ss.Err())
			require.Contains(t, ss.Err().Error(), expectedError)
		})

		t.Run("labelNames should return consistency check error", func(t *testing.T) {
			_, _, err := pq.LabelNames(ctx, nil, matchers...)
			require.Error(t, err)
			require.Contains(t, err.Error(), expectedError)
		})

		t.Run("labelValues should return consistency check error", func(t *testing.T) {
			_, _, err := pq.LabelValues(ctx, labels.MetricName, nil, matchers...)
			require.Error(t, err)
			require.Contains(t, err.Error(), expectedError)
		})
	})

	t.Run("should work normally when all blocks are available as parquet and fallback disabled", func(t *testing.T) {
		finder := &blocksFinderMock{}
		stores := createStore()

		q := &blocksStoreQuerier{
			minT:        minT,
			maxT:        maxT,
			finder:      finder,
			stores:      stores,
			consistency: NewBlocksConsistencyChecker(0, 0, log.NewNopLogger(), nil),
			logger:      log.NewNopLogger(),
			metrics:     newBlocksStoreQueryableMetrics(prometheus.NewPedanticRegistry()),
			limits:      &blocksStoreLimitsMock{},

			storeGatewayConsistencyCheckMaxAttempts: 3,
		}

		mParquetQuerier := &mockParquetQuerier{}
		pq := &parquetQuerierWithFallback{
			minT:                  minT,
			maxT:                  maxT,
			finder:                finder,
			blocksStoreQuerier:    q,
			parquetQuerier:        mParquetQuerier,
			queryStoreAfter:       time.Hour,
			metrics:               newParquetQueryableFallbackMetrics(prometheus.NewRegistry()),
			limits:                defaultOverrides(t, 0),
			logger:                log.NewNopLogger(),
			defaultBlockStoreType: parquetBlockStore,
			fallbackDisabled:      true, // Disable fallback
		}

		// Set up blocks where both blocks have parquet metadata
		finder.On("GetBlocks", mock.Anything, "user-1", minT, mock.Anything, mock.Anything).Return(bucketindex.Blocks{
			&bucketindex.Block{ID: block1, Parquet: &parquet.ConverterMarkMeta{Version: 1}}, // Available as parquet
			&bucketindex.Block{ID: block2, Parquet: &parquet.ConverterMarkMeta{Version: 1}}, // Available as parquet
		}, map[ulid.ULID]*bucketindex.BlockDeletionMark(nil), nil)

		t.Run("select should work without error", func(t *testing.T) {
			mParquetQuerier.Reset()
			ss := pq.Select(ctx, true, nil, matchers...)
			require.NoError(t, ss.Err())
			require.Len(t, mParquetQuerier.queriedBlocks, 2)
		})

		t.Run("labelNames should work without error", func(t *testing.T) {
			mParquetQuerier.Reset()
			_, _, err := pq.LabelNames(ctx, nil, matchers...)
			require.NoError(t, err)
			require.Len(t, mParquetQuerier.queriedBlocks, 2)
		})

		t.Run("labelValues should work without error", func(t *testing.T) {
			mParquetQuerier.Reset()
			_, _, err := pq.LabelValues(ctx, labels.MetricName, nil, matchers...)
			require.NoError(t, err)
			require.Len(t, mParquetQuerier.queriedBlocks, 2)
		})
	})
}
