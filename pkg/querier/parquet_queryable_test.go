package querier

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/querier/series"
	"github.com/cortexproject/cortex/pkg/storage/parquet"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
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
						mockSeriesResponse(labels.Labels{{Name: labels.MetricName, Value: "fromSg"}}, []cortexpb.Sample{{Value: 1, TimestampMs: minT}, {Value: 2, TimestampMs: minT + 1}}, nil, nil),
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

	t.Run("should fallback when vertical sharding is enabled", func(t *testing.T) {
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
			limits:                defaultOverrides(t, 4),
			logger:                log.NewNopLogger(),
			defaultBlockStoreType: parquetBlockStore,
		}

		finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT).Return(bucketindex.Blocks{
			&bucketindex.Block{ID: block1, Parquet: &parquet.ConverterMarkMeta{Version: 1}},
			&bucketindex.Block{ID: block2, Parquet: &parquet.ConverterMarkMeta{Version: 1}},
		}, map[ulid.ULID]*bucketindex.BlockDeletionMark(nil), nil)

		t.Run("select", func(t *testing.T) {
			ss := pq.Select(ctx, true, nil, matchers...)
			require.NoError(t, ss.Err())
			require.Len(t, stores.queriedBlocks, 2)
			require.Len(t, mParquetQuerier.queriedBlocks, 0)
		})
	})

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

		finder.On("GetBlocks", mock.Anything, "user-1", minT, mock.Anything).Return(bucketindex.Blocks{
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

		finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT).Return(bucketindex.Blocks{
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

		finder.On("GetBlocks", mock.Anything, "user-1", minT, mock.Anything).Return(bucketindex.Blocks{
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

		finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT).Return(bucketindex.Blocks{
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
