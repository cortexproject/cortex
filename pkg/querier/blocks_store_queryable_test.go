package querier

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/gogo/protobuf/types"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/store/hintspb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/weaveworks/common/user"
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
		finderResult      []*BlockMeta
		finderErr         error
		storeSetResponses []interface{}
		expectedSeries    []seriesResult
		expectedErr       string
		expectedMetrics   string
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
			storeSetResponses: []interface{}{
				errors.New("no client found"),
			},
			expectedErr: "no client found",
		},
		"a single store-gateway instance holds the required blocks (single returned series)": {
			finderResult: []*BlockMeta{
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block1}}},
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block2}}},
			},
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel}, minT, 1),
						mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
						mockHintsResponse(block1, block2),
					}}: {block1, block2},
				},
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
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT, 1),
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT+1, 2),
						mockSeriesResponse(labels.Labels{metricNameLabel, series2Label}, minT, 3),
						mockHintsResponse(block1, block2),
					}}: {block1, block2},
				},
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
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel}, minT, 1),
						mockHintsResponse(block1),
					}}: {block1},
					&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
						mockHintsResponse(block2),
					}}: {block2},
				},
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
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
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
			storeSetResponses: []interface{}{
				map[BlocksStoreClient][]ulid.ULID{
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
			expectedMetrics: `
				# HELP cortex_querier_storegateway_instances_hit_per_query Number of store-gateway instances hit for a single query.
				# TYPE cortex_querier_storegateway_instances_hit_per_query histogram
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="0"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="3"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="5"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="6"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="7"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="9"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="10"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_instances_hit_per_query_sum 3
				cortex_querier_storegateway_instances_hit_per_query_count 1

				# HELP cortex_querier_storegateway_refetches_per_query Number of re-fetches attempted while querying store-gateway instances due to missing blocks.
				# TYPE cortex_querier_storegateway_refetches_per_query histogram
				cortex_querier_storegateway_refetches_per_query_bucket{le="0"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="1"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="2"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_refetches_per_query_sum 0
				cortex_querier_storegateway_refetches_per_query_count 1
			`,
		},
		"a single store-gateway instance has some missing blocks (consistency check failed)": {
			finderResult: []*BlockMeta{
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block1}}},
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block2}}},
			},
			storeSetResponses: []interface{}{
				// First attempt returns a client whose response does not include all expected blocks.
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT, 1),
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT+1, 2),
						mockHintsResponse(block1),
					}}: {block1},
				},
				// Second attempt returns an error because there are no other store-gateways left.
				errors.New("no store-gateway remaining after exclude"),
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
			storeSetResponses: []interface{}{
				// First attempt returns a client whose response does not include all expected blocks.
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
						mockHintsResponse(block1),
					}}: {block1},
					&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
						mockHintsResponse(block2),
					}}: {block2},
				},
				// Second attempt returns an error because there are no other store-gateways left.
				errors.New("no store-gateway remaining after exclude"),
			},
			expectedErr: fmt.Sprintf("consistency check failed because some blocks were not queried: %s %s", block3.String(), block4.String()),
		},
		"multiple store-gateway instances have some missing blocks but queried from a replica during subsequent attempts": {
			finderResult: []*BlockMeta{
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block1}}},
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block2}}},
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block3}}},
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block4}}},
			},
			storeSetResponses: []interface{}{
				// First attempt returns a client whose response does not include all expected blocks.
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT, 1),
						mockHintsResponse(block1),
					}}: {block1, block3},
					&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series2Label}, minT, 2),
						mockHintsResponse(block2),
					}}: {block2, block4},
				},
				// Second attempt returns 1 missing block.
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "3.3.3.3", mockedResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT+1, 2),
						mockHintsResponse(block3),
					}}: {block3, block4},
				},
				// Third attempt returns the last missing block.
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{remoteAddr: "4.4.4.4", mockedResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.Labels{metricNameLabel, series2Label}, minT+1, 3),
						mockHintsResponse(block4),
					}}: {block4},
				},
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
						{t: minT, v: 2},
						{t: minT + 1, v: 3},
					},
				},
			},
			expectedMetrics: `
				# HELP cortex_querier_storegateway_instances_hit_per_query Number of store-gateway instances hit for a single query.
				# TYPE cortex_querier_storegateway_instances_hit_per_query histogram
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="0"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="1"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="2"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="3"} 0
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="4"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="5"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="6"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="7"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="8"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="9"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="10"} 1
				cortex_querier_storegateway_instances_hit_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_instances_hit_per_query_sum 4
				cortex_querier_storegateway_instances_hit_per_query_count 1

				# HELP cortex_querier_storegateway_refetches_per_query Number of re-fetches attempted while querying store-gateway instances due to missing blocks.
				# TYPE cortex_querier_storegateway_refetches_per_query histogram
				cortex_querier_storegateway_refetches_per_query_bucket{le="0"} 0
				cortex_querier_storegateway_refetches_per_query_bucket{le="1"} 0
				cortex_querier_storegateway_refetches_per_query_bucket{le="2"} 1
				cortex_querier_storegateway_refetches_per_query_bucket{le="+Inf"} 1
				cortex_querier_storegateway_refetches_per_query_sum 2
				cortex_querier_storegateway_refetches_per_query_count 1
			`,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := context.Background()
			reg := prometheus.NewPedanticRegistry()
			stores := &blocksStoreSetMock{mockedResponses: testData.storeSetResponses}
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
				metrics:     newBlocksStoreQueryableMetrics(reg),
			}

			matchers := []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metricName),
			}

			set := q.Select(true, nil, matchers...)
			if testData.expectedErr != "" {
				assert.EqualError(t, set.Err(), testData.expectedErr)
				assert.False(t, set.Next())
				assert.Nil(t, set.Warnings())
				return
			}

			require.NoError(t, set.Err())
			assert.Len(t, set.Warnings(), 0)

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

			// Assert on metrics (optional, only for test cases defining it).
			if testData.expectedMetrics != "" {
				assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(testData.expectedMetrics)))
			}
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
				metrics:         newBlocksStoreQueryableMetrics(nil),
				queryStoreAfter: testData.queryStoreAfter,
			}

			sp := &storage.SelectHints{
				Start: testData.queryMinT,
				End:   testData.queryMaxT,
			}

			set := q.selectSorted(sp, nil)
			require.NoError(t, set.Err())

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

func TestBlocksStoreQuerier_PromQLExecution(t *testing.T) {
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	series1 := []storepb.Label{{Name: "__name__", Value: "metric_1"}}
	series2 := []storepb.Label{{Name: "__name__", Value: "metric_2"}}

	series1Samples := []promql.Point{
		{T: 1589759955000, V: 1},
		{T: 1589759970000, V: 1},
		{T: 1589759985000, V: 1},
		{T: 1589760000000, V: 1},
		{T: 1589760015000, V: 1},
		{T: 1589760030000, V: 1},
	}

	series2Samples := []promql.Point{
		{T: 1589759955000, V: 2},
		{T: 1589759970000, V: 2},
		{T: 1589759985000, V: 2},
		{T: 1589760000000, V: 2},
		{T: 1589760015000, V: 2},
		{T: 1589760030000, V: 2},
	}

	// Mock the finder to simulate we need to query two blocks.
	finder := &blocksFinderMock{
		Service: services.NewIdleService(nil, nil),
	}
	finder.On("GetBlocks", "user-1", mock.Anything, mock.Anything).Return([]*BlockMeta{
		{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block1}}},
		{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block2}}},
	}, map[ulid.ULID]*metadata.DeletionMark(nil), error(nil))

	// Mock the store to simulate each block is queried from a different store-gateway.
	gateway1 := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedResponses: []*storepb.SeriesResponse{
		{
			Result: &storepb.SeriesResponse_Series{
				Series: &storepb.Series{
					Labels: series1,
					Chunks: []storepb.AggrChunk{
						createAggrChunkWithSamples(series1Samples[:3]...), // First half.
					},
				},
			},
		}, {
			Result: &storepb.SeriesResponse_Series{
				Series: &storepb.Series{
					Labels: series2,
					Chunks: []storepb.AggrChunk{
						createAggrChunkWithSamples(series2Samples[:3]...),
					},
				},
			},
		},
		mockHintsResponse(block1),
	}}

	gateway2 := &storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedResponses: []*storepb.SeriesResponse{
		{
			Result: &storepb.SeriesResponse_Series{
				Series: &storepb.Series{
					Labels: series1,
					Chunks: []storepb.AggrChunk{
						createAggrChunkWithSamples(series1Samples[3:]...), // Second half.
					},
				},
			},
		}, {
			Result: &storepb.SeriesResponse_Series{
				Series: &storepb.Series{
					Labels: series2,
					Chunks: []storepb.AggrChunk{
						createAggrChunkWithSamples(series2Samples[3:]...),
					},
				},
			},
		},
		mockHintsResponse(block2),
	}}

	stores := &blocksStoreSetMock{
		Service: services.NewIdleService(nil, nil),
		mockedResponses: []interface{}{
			map[BlocksStoreClient][]ulid.ULID{
				gateway1: {block1},
				gateway2: {block2},
			},
		},
	}

	// Instance the querier that will be executed to run the query.
	logger := log.NewNopLogger()
	queryable, err := NewBlocksStoreQueryable(stores, finder, NewBlocksConsistencyChecker(0, 0, logger, nil), 0, logger, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), queryable))
	defer services.StopAndAwaitTerminated(context.Background(), queryable) // nolint:errcheck

	engine := promql.NewEngine(promql.EngineOpts{
		Logger:     logger,
		Timeout:    10 * time.Second,
		MaxSamples: 1e6,
	})

	// Run a query.
	q, err := engine.NewRangeQuery(queryable, `{__name__=~"metric.*"}`, time.Unix(1589759955, 0), time.Unix(1589760030, 0), 15*time.Second)
	require.NoError(t, err)

	ctx := user.InjectOrgID(context.Background(), "user-1")
	res := q.Exec(ctx)
	require.NoError(t, err)
	require.NoError(t, res.Err)

	matrix, err := res.Matrix()
	require.NoError(t, err)
	require.Len(t, matrix, 2)

	assert.Equal(t, storepb.LabelsToPromLabels(series1), matrix[0].Metric)
	assert.Equal(t, storepb.LabelsToPromLabels(series2), matrix[1].Metric)
	assert.Equal(t, series1Samples, matrix[0].Points)
	assert.Equal(t, series2Samples, matrix[1].Points)
}

type blocksStoreSetMock struct {
	services.Service

	mockedResponses []interface{}
	nextResult      int
}

func (m *blocksStoreSetMock) GetClientsFor(_ []ulid.ULID, _ map[ulid.ULID][]string) (map[BlocksStoreClient][]ulid.ULID, error) {
	if m.nextResult >= len(m.mockedResponses) {
		panic("not enough mocked results")
	}

	res := m.mockedResponses[m.nextResult]
	m.nextResult++

	if err, ok := res.(error); ok {
		return nil, err
	}
	if clients, ok := res.(map[BlocksStoreClient][]ulid.ULID); ok {
		return clients, nil
	}

	return nil, errors.New("unknown data type in the mocked result")
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
