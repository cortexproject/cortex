package querier

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/gogo/protobuf/types"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/store/hintspb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/storegateway/storegatewaypb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestBlocksStoreQuerier_SelectSorted(t *testing.T) {
	const (
		metricName = "test_metric"
		minT       = int64(10)
		maxT       = int64(20)
	)

	var (
		block1          = ulid.MustNew(1, nil)
		block2          = ulid.MustNew(2, nil)
		block3          = ulid.MustNew(3, nil)
		block4          = ulid.MustNew(4, nil)
		metricNameLabel = labels.Label{Name: labels.MetricName, Value: metricName}
		series1Label    = labels.Label{Name: "series", Value: "1"}
		series2Label    = labels.Label{Name: "series", Value: "2"}
	)

	type valueResult struct {
		t int64
		v float64
	}

	type seriesResult struct {
		lbls   labels.Labels
		values []valueResult
	}

	tests := map[string]struct {
		finderResult   []*BlockMeta
		finderErr      error
		storeSetResult map[BlocksStoreClient][]ulid.ULID
		storeSetErr    error
		expectedSeries []seriesResult
		expectedErr    string
	}{
		"no block in the storage matching the query time range": {
			finderResult: nil,
			expectedErr:  "",
		},
		"error while finding blocks matching the query time range": {
			finderErr:   errors.New("unable to find blocks"),
			expectedErr: "unable to find blocks",
		},
		"error while getting clients to query the store-gateway": {
			finderResult: []*BlockMeta{
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block1}}},
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block2}}},
			},
			storeSetErr: errors.New("no client found"),
			expectedErr: "no client found",
		},
		"a single store-gateway instance holds the required blocks (single returned series)": {
			finderResult: []*BlockMeta{
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block1}}},
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block2}}},
			},
			storeSetResult: map[BlocksStoreClient][]ulid.ULID{
				&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel}, minT, 1),
					mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
					mockHintsResponse(block1, block2),
				}}: {block1, block2},
			},
			expectedSeries: []seriesResult{
				{
					lbls: labels.New(metricNameLabel),
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 2},
					},
				},
			},
		},
		"a single store-gateway instance holds the required blocks (multiple returned series)": {
			finderResult: []*BlockMeta{
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block1}}},
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block2}}},
			},
			storeSetResult: map[BlocksStoreClient][]ulid.ULID{
				&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT, 1),
					mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT+1, 2),
					mockSeriesResponse(labels.Labels{metricNameLabel, series2Label}, minT, 3),
					mockHintsResponse(block1, block2),
				}}: {block1, block2},
			},
			expectedSeries: []seriesResult{
				{
					lbls: labels.New(metricNameLabel, series1Label),
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 2},
					},
				}, {
					lbls: labels.New(metricNameLabel, series2Label),
					values: []valueResult{
						{t: minT, v: 3},
					},
				},
			},
		},
		"multiple store-gateway instances holds the required blocks without overlapping series (single returned series)": {
			finderResult: []*BlockMeta{
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block1}}},
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block2}}},
			},
			storeSetResult: map[BlocksStoreClient][]ulid.ULID{
				&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel}, minT, 1),
					mockHintsResponse(block1),
				}}: {block1},
				&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
					mockHintsResponse(block2),
				}}: {block2},
			},
			expectedSeries: []seriesResult{
				{
					lbls: labels.New(metricNameLabel),
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 2},
					},
				},
			},
		},
		"multiple store-gateway instances holds the required blocks with overlapping series (single returned series)": {
			finderResult: []*BlockMeta{
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block1}}},
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block2}}},
			},
			storeSetResult: map[BlocksStoreClient][]ulid.ULID{
				&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
					mockHintsResponse(block1),
				}}: {block1},
				&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel}, minT, 1),
					mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
					mockHintsResponse(block2),
				}}: {block2},
			},
			expectedSeries: []seriesResult{
				{
					lbls: labels.New(metricNameLabel),
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 2},
					},
				},
			},
		},
		"multiple store-gateway instances holds the required blocks with overlapping series (multiple returned series)": {
			finderResult: []*BlockMeta{
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block1}}},
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block2}}},
			},
			storeSetResult: map[BlocksStoreClient][]ulid.ULID{
				&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT+1, 2),
					mockSeriesResponse(labels.Labels{metricNameLabel, series2Label}, minT, 1),
					mockHintsResponse(block1),
				}}: {block1},
				&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT, 1),
					mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT+1, 2),
					mockHintsResponse(block2),
				}}: {block2},
				&storeGatewayClientMock{remoteAddr: "3.3.3.3", mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel, series2Label}, minT, 1),
					mockSeriesResponse(labels.Labels{metricNameLabel, series2Label}, minT+1, 3),
					mockHintsResponse(block3),
				}}: {block3},
			},
			expectedSeries: []seriesResult{
				{
					lbls: labels.New(metricNameLabel, series1Label),
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 2},
					},
				}, {
					lbls: labels.New(metricNameLabel, series2Label),
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 3},
					},
				},
			},
		},
		"a single store-gateway instance has some missing blocks (consistency check failed)": {
			finderResult: []*BlockMeta{
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block1}}},
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block2}}},
			},
			storeSetResult: map[BlocksStoreClient][]ulid.ULID{
				&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT, 1),
					mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT+1, 2),
					mockHintsResponse(block1),
				}}: {block1},
			},
			expectedErr: fmt.Sprintf("consistency check failed because some blocks were not queried: %s", block2.String()),
		},
		"multiple store-gateway instances have some missing blocks (consistency check failed)": {
			finderResult: []*BlockMeta{
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block1}}},
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block2}}},
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block3}}},
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block4}}},
			},
			storeSetResult: map[BlocksStoreClient][]ulid.ULID{
				&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
					mockHintsResponse(block1),
				}}: {block1},
				&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
					mockHintsResponse(block2),
				}}: {block2},
			},
			expectedErr: fmt.Sprintf("consistency check failed because some blocks were not queried: %s %s", block3.String(), block4.String()),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := context.Background()
			stores := &blocksStoreSetMock{
				mockedResult: testData.storeSetResult,
				mockedErr:    testData.storeSetErr,
			}
			finder := &blocksFinderMock{}
			finder.On("GetBlocks", "user-1", minT, maxT).Return(testData.finderResult, map[ulid.ULID]*metadata.DeletionMark(nil), testData.finderErr)

			q := &blocksStoreQuerier{
				ctx:         ctx,
				minT:        minT,
				maxT:        maxT,
				userID:      "user-1",
				finder:      finder,
				stores:      stores,
				consistency: NewBlocksConsistencyChecker(0, 0, log.NewNopLogger(), nil),
				logger:      log.NewNopLogger(),
				storesHit:   prometheus.NewHistogram(prometheus.HistogramOpts{}),
			}

			matchers := []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metricName),
			}

			set, warnings, err := q.Select(true, nil, matchers...)
			if testData.expectedErr != "" {
				assert.EqualError(t, err, testData.expectedErr)
				assert.Nil(t, set)
				assert.Nil(t, warnings)
				return
			}

			require.NoError(t, err)
			assert.Len(t, warnings, 0)

			// Read all returned series and their values.
			var actualSeries []seriesResult
			for set.Next() {
				var actualValues []valueResult

				it := set.At().Iterator()
				for it.Next() {
					t, v := it.At()
					actualValues = append(actualValues, valueResult{
						t: t,
						v: v,
					})
				}

				require.NoError(t, it.Err())

				actualSeries = append(actualSeries, seriesResult{
					lbls:   set.At().Labels(),
					values: actualValues,
				})
			}
			require.NoError(t, set.Err())
			assert.Equal(t, testData.expectedSeries, actualSeries)
		})
	}
}

func TestBlocksStoreQuerier_SelectSortedShouldHonorQueryStoreAfter(t *testing.T) {
	now := time.Now()

	tests := map[string]struct {
		queryStoreAfter time.Duration
		queryMinT       int64
		queryMaxT       int64
		expectedMinT    int64
		expectedMaxT    int64
	}{
		"should not manipulate query time range if queryStoreAfter is disabled": {
			queryStoreAfter: 0,
			queryMinT:       util.TimeToMillis(now.Add(-100 * time.Minute)),
			queryMaxT:       util.TimeToMillis(now.Add(-30 * time.Minute)),
			expectedMinT:    util.TimeToMillis(now.Add(-100 * time.Minute)),
			expectedMaxT:    util.TimeToMillis(now.Add(-30 * time.Minute)),
		},
		"should not manipulate query time range if queryStoreAfter is enabled but query max time is older": {
			queryStoreAfter: time.Hour,
			queryMinT:       util.TimeToMillis(now.Add(-100 * time.Minute)),
			queryMaxT:       util.TimeToMillis(now.Add(-70 * time.Minute)),
			expectedMinT:    util.TimeToMillis(now.Add(-100 * time.Minute)),
			expectedMaxT:    util.TimeToMillis(now.Add(-70 * time.Minute)),
		},
		"should manipulate query time range if queryStoreAfter is enabled and query max time is recent": {
			queryStoreAfter: time.Hour,
			queryMinT:       util.TimeToMillis(now.Add(-100 * time.Minute)),
			queryMaxT:       util.TimeToMillis(now.Add(-30 * time.Minute)),
			expectedMinT:    util.TimeToMillis(now.Add(-100 * time.Minute)),
			expectedMaxT:    util.TimeToMillis(now.Add(-60 * time.Minute)),
		},
		"should skip the query if the query min time is more recent than queryStoreAfter": {
			queryStoreAfter: time.Hour,
			queryMinT:       util.TimeToMillis(now.Add(-50 * time.Minute)),
			queryMaxT:       util.TimeToMillis(now.Add(-20 * time.Minute)),
			expectedMinT:    0,
			expectedMaxT:    0,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			finder := &blocksFinderMock{}
			finder.On("GetBlocks", "user-1", mock.Anything, mock.Anything).Return([]*BlockMeta(nil), map[ulid.ULID]*metadata.DeletionMark(nil), error(nil))

			q := &blocksStoreQuerier{
				ctx:             context.Background(),
				minT:            testData.queryMinT,
				maxT:            testData.queryMaxT,
				userID:          "user-1",
				finder:          finder,
				stores:          &blocksStoreSetMock{},
				consistency:     NewBlocksConsistencyChecker(0, 0, log.NewNopLogger(), nil),
				logger:          log.NewNopLogger(),
				storesHit:       prometheus.NewHistogram(prometheus.HistogramOpts{}),
				queryStoreAfter: testData.queryStoreAfter,
			}

			sp := &storage.SelectHints{
				Start: testData.queryMinT,
				End:   testData.queryMaxT,
			}

			_, _, err := q.selectSorted(sp, nil)
			require.NoError(t, err)

			if testData.expectedMinT == 0 && testData.expectedMaxT == 0 {
				assert.Len(t, finder.Calls, 0)
			} else {
				require.Len(t, finder.Calls, 1)
				assert.Equal(t, testData.expectedMinT, finder.Calls[0].Arguments.Get(1))
				assert.InDelta(t, testData.expectedMaxT, finder.Calls[0].Arguments.Get(2), float64(5*time.Second.Milliseconds()))
			}
		})
	}
}

type blocksStoreSetMock struct {
	services.Service

	mockedResult map[BlocksStoreClient][]ulid.ULID
	mockedErr    error
}

func (m *blocksStoreSetMock) GetClientsFor(_ []ulid.ULID) (map[BlocksStoreClient][]ulid.ULID, error) {
	return m.mockedResult, m.mockedErr
}

type blocksFinderMock struct {
	services.Service
	mock.Mock
}

func (m *blocksFinderMock) GetBlocks(userID string, minT, maxT int64) ([]*BlockMeta, map[ulid.ULID]*metadata.DeletionMark, error) {
	args := m.Called(userID, minT, maxT)
	return args.Get(0).([]*BlockMeta), args.Get(1).(map[ulid.ULID]*metadata.DeletionMark), args.Error(2)
}

type storeGatewayClientMock struct {
	remoteAddr      string
	mockedResponses []*storepb.SeriesResponse
}

func (m *storeGatewayClientMock) Series(ctx context.Context, in *storepb.SeriesRequest, opts ...grpc.CallOption) (storegatewaypb.StoreGateway_SeriesClient, error) {
	seriesClient := &storeGatewaySeriesClientMock{
		mockedResponses: m.mockedResponses,
	}

	return seriesClient, nil
}

func (m *storeGatewayClientMock) RemoteAddress() string {
	return m.remoteAddr
}

type storeGatewaySeriesClientMock struct {
	grpc.ClientStream

	mockedResponses []*storepb.SeriesResponse
}

func (m *storeGatewaySeriesClientMock) Recv() (*storepb.SeriesResponse, error) {
	// Ensure some concurrency occurs.
	time.Sleep(10 * time.Millisecond)

	if len(m.mockedResponses) == 0 {
		return nil, io.EOF
	}

	res := m.mockedResponses[0]
	m.mockedResponses = m.mockedResponses[1:]
	return res, nil
}

func mockSeriesResponse(lbls labels.Labels, timeMillis int64, value float64) *storepb.SeriesResponse {
	// Generate a chunk containing a single value (for simplicity).
	chunk := chunkenc.NewXORChunk()
	appender, err := chunk.Appender()
	if err != nil {
		panic(err)
	}
	appender.Append(timeMillis, value)
	chunkData := chunk.Bytes()

	return &storepb.SeriesResponse{
		Result: &storepb.SeriesResponse_Series{
			Series: &storepb.Series{
				Labels: storepb.PromLabelsToLabels(lbls),
				Chunks: []storepb.AggrChunk{
					{MinTime: timeMillis, MaxTime: timeMillis, Raw: &storepb.Chunk{Type: storepb.Chunk_XOR, Data: chunkData}},
				},
			},
		},
	}
}

func mockHintsResponse(ids ...ulid.ULID) *storepb.SeriesResponse {
	hints := &hintspb.SeriesResponseHints{}
	for _, id := range ids {
		hints.AddQueriedBlock(id)
	}

	any, err := types.MarshalAny(hints)
	if err != nil {
		panic(err)
	}

	return &storepb.SeriesResponse{
		Result: &storepb.SeriesResponse_Hints{
			Hints: any,
		},
	}
}
