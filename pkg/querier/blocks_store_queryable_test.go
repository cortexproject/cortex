package querier

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/storegateway/storegatewaypb"
	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestBlocksStoreQuerier_SelectSorted(t *testing.T) {
	const (
		metricName = "test_metric"
		minT       = 10
		maxT       = 20
	)

	var (
		block1          = ulid.MustNew(1, nil)
		block2          = ulid.MustNew(2, nil)
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
		finderResult   []*metadata.Meta
		finderErr      error
		storeSetResult []storegatewaypb.StoreGatewayClient
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
			finderResult: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2}},
			},
			storeSetErr: errors.New("no client found"),
			expectedErr: "no client found",
		},
		"a single store-gateway instance holds the required blocks (single returned series)": {
			finderResult: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2}},
			},
			storeSetResult: []storegatewaypb.StoreGatewayClient{
				&storeGatewayClientMock{mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel}, minT, 1),
					mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
				}},
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
			finderResult: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2}},
			},
			storeSetResult: []storegatewaypb.StoreGatewayClient{
				&storeGatewayClientMock{mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT, 1),
					mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT+1, 2),
					mockSeriesResponse(labels.Labels{metricNameLabel, series2Label}, minT, 3),
				}},
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
		"multiple store-gateway instances holds the required blocks without overlapping blocks (single returned series)": {
			finderResult: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2}},
			},
			storeSetResult: []storegatewaypb.StoreGatewayClient{
				&storeGatewayClientMock{mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel}, minT, 1),
				}},
				&storeGatewayClientMock{mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
				}},
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
		"multiple store-gateway instances holds the required blocks with overlapping blocks (single returned series)": {
			finderResult: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2}},
			},
			storeSetResult: []storegatewaypb.StoreGatewayClient{
				&storeGatewayClientMock{mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
				}},
				&storeGatewayClientMock{mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel}, minT, 1),
					mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
				}},
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
		"multiple store-gateway instances holds the required blocks with overlapping blocks (multiple returned series)": {
			finderResult: []*metadata.Meta{
				{BlockMeta: tsdb.BlockMeta{ULID: block1}},
				{BlockMeta: tsdb.BlockMeta{ULID: block2}},
			},
			storeSetResult: []storegatewaypb.StoreGatewayClient{
				&storeGatewayClientMock{mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT+1, 2),
					mockSeriesResponse(labels.Labels{metricNameLabel, series2Label}, minT, 1),
				}},
				&storeGatewayClientMock{mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT, 1),
					mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT+1, 2),
				}},
				&storeGatewayClientMock{mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel, series2Label}, minT, 1),
					mockSeriesResponse(labels.Labels{metricNameLabel, series2Label}, minT+1, 3),
				}},
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
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := context.Background()
			stores := &blocksStoreSetMock{
				mockedResult: testData.storeSetResult,
				mockedErr:    testData.storeSetErr,
			}
			finder := &blocksFinderMock{
				mockedResult: testData.finderResult,
				mockedErr:    testData.finderErr,
			}

			q := &blocksStoreQuerier{
				ctx:    ctx,
				minT:   minT,
				maxT:   maxT,
				userID: "user-1",
				finder: finder,
				stores: stores,
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

type blocksStoreSetMock struct {
	services.Service

	mockedResult []storegatewaypb.StoreGatewayClient
	mockedErr    error
}

func (m *blocksStoreSetMock) GetClientsFor(metas []*metadata.Meta) ([]storegatewaypb.StoreGatewayClient, error) {
	return m.mockedResult, m.mockedErr
}

type blocksFinderMock struct {
	services.Service

	mockedResult []*metadata.Meta
	mockedErr    error
}

func (m *blocksFinderMock) GetBlocks(userID string, minT, maxT int64) ([]*metadata.Meta, error) {
	return m.mockedResult, m.mockedErr
}

type storeGatewayClientMock struct {
	mockedResponses []*storepb.SeriesResponse
}

func (m *storeGatewayClientMock) Series(ctx context.Context, in *storepb.SeriesRequest, opts ...grpc.CallOption) (storegatewaypb.StoreGateway_SeriesClient, error) {
	seriesClient := &storeGatewaySeriesClientMock{
		mockedResponses: m.mockedResponses,
	}

	return seriesClient, nil
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
