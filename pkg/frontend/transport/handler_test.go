package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc/codes"

	querier_stats "github.com/cortexproject/cortex/pkg/querier/stats"
	util_api "github.com/cortexproject/cortex/pkg/util/api"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestWriteError(t *testing.T) {
	for _, test := range []struct {
		status            int
		err               error
		additionalHeaders http.Header
		expectedErrResp   util_api.Response
	}{
		{
			http.StatusInternalServerError,
			errors.New("unknown"),
			http.Header{"User-Agent": []string{"Golang"}},
			util_api.Response{
				Status:    "error",
				ErrorType: v1.ErrServer,
				Error:     "unknown",
			},
		},
		{
			http.StatusInternalServerError,
			errors.New("unknown"),
			nil,
			util_api.Response{
				Status:    "error",
				ErrorType: v1.ErrServer,
				Error:     "unknown",
			},
		},
		{
			http.StatusGatewayTimeout,
			context.DeadlineExceeded,
			nil,
			util_api.Response{
				Status:    "error",
				ErrorType: v1.ErrTimeout,
				Error:     "",
			},
		},
		{
			StatusClientClosedRequest,
			context.Canceled,
			nil,
			util_api.Response{
				Status:    "error",
				ErrorType: v1.ErrCanceled,
				Error:     "",
			},
		},
		{
			StatusClientClosedRequest,
			context.Canceled,
			http.Header{"User-Agent": []string{"Golang"}},
			util_api.Response{
				Status:    "error",
				ErrorType: v1.ErrCanceled,
				Error:     "",
			},
		},
		{
			StatusClientClosedRequest,
			context.Canceled,
			http.Header{"User-Agent": []string{"Golang"}, "Content-Type": []string{"application/json"}},
			util_api.Response{
				Status:    "error",
				ErrorType: v1.ErrCanceled,
				Error:     "",
			},
		},
		{http.StatusBadRequest,
			httpgrpc.Errorf(http.StatusBadRequest, ""),
			http.Header{},
			util_api.Response{
				Status:    "error",
				ErrorType: v1.ErrBadData,
				Error:     "",
			},
		},
		{
			http.StatusRequestEntityTooLarge,
			errors.New("http: request body too large"),
			http.Header{},
			util_api.Response{
				Status:    "error",
				ErrorType: v1.ErrBadData,
				Error:     "http: request body too large",
			},
		},
		{
			http.StatusUnprocessableEntity,
			httpgrpc.Errorf(http.StatusUnprocessableEntity, "limit hit"),
			http.Header{},
			util_api.Response{
				Status:    "error",
				ErrorType: v1.ErrExec,
				Error:     "limit hit",
			},
		},
		{
			http.StatusUnprocessableEntity,
			httpgrpc.Errorf(int(codes.PermissionDenied), "permission denied"),
			http.Header{},
			util_api.Response{
				Status:    "error",
				ErrorType: v1.ErrBadData,
				Error:     "permission denied",
			},
		},
	} {
		t.Run(test.err.Error(), func(t *testing.T) {
			w := httptest.NewRecorder()
			writeError(util_log.Logger, w, test.err, test.additionalHeaders)
			require.Equal(t, test.status, w.Result().StatusCode)
			expectedAdditionalHeaders := test.additionalHeaders
			if expectedAdditionalHeaders != nil {
				for key, value := range w.Header() {
					if values, ok := expectedAdditionalHeaders[key]; ok {
						require.Equal(t, values, value)
					}
				}
			}
			data, err := io.ReadAll(w.Result().Body)
			require.NoError(t, err)
			var res util_api.Response
			err = json.Unmarshal(data, &res)
			require.NoError(t, err)
			resp, ok := httpgrpc.HTTPResponseFromError(test.err)
			if ok {
				require.Equal(t, string(resp.Body), res.Error)
			} else {
				require.Equal(t, test.err.Error(), res.Error)

			}
		})
	}
}

func TestHandler_ServeHTTP(t *testing.T) {
	roundTripper := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("{}")),
		}, nil
	})
	userID := "12345"
	for _, tt := range []struct {
		name                       string
		cfg                        HandlerConfig
		expectedMetrics            int
		expectedStatusCode         int
		roundTripperFunc           roundTripperFunc
		additionalMetricsCheckFunc func(h *Handler)
	}{
		{
			name:               "test handler with stats enabled",
			cfg:                HandlerConfig{QueryStatsEnabled: true},
			expectedMetrics:    6,
			roundTripperFunc:   roundTripper,
			expectedStatusCode: http.StatusOK,
		},
		{
			name:               "test handler with stats disabled",
			cfg:                HandlerConfig{QueryStatsEnabled: false},
			expectedMetrics:    0,
			roundTripperFunc:   roundTripper,
			expectedStatusCode: http.StatusOK,
		},
		{
			name:            "test handler with reasonResponseTooLarge",
			cfg:             HandlerConfig{QueryStatsEnabled: true},
			expectedMetrics: 6,
			roundTripperFunc: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusRequestEntityTooLarge,
					Body:       io.NopCloser(strings.NewReader("{}")),
				}, nil
			}),
			additionalMetricsCheckFunc: func(h *Handler) {
				v := promtest.ToFloat64(h.rejectedQueries.WithLabelValues(reasonResponseBodySizeExceeded, userID))
				assert.Equal(t, float64(1), v)
			},
			expectedStatusCode: http.StatusRequestEntityTooLarge,
		},
		{
			name:            "test handler with reasonTooManyRequests",
			cfg:             HandlerConfig{QueryStatsEnabled: true},
			expectedMetrics: 6,
			roundTripperFunc: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusTooManyRequests,
					Body:       io.NopCloser(strings.NewReader("{}")),
				}, nil
			}),
			additionalMetricsCheckFunc: func(h *Handler) {
				v := promtest.ToFloat64(h.rejectedQueries.WithLabelValues(reasonTooManyRequests, userID))
				assert.Equal(t, float64(1), v)
			},
			expectedStatusCode: http.StatusTooManyRequests,
		},
		{
			name:            "test handler with reasonTooManySamples",
			cfg:             HandlerConfig{QueryStatsEnabled: true},
			expectedMetrics: 6,
			roundTripperFunc: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusUnprocessableEntity,
					Body:       io.NopCloser(strings.NewReader(limitTooManySamples)),
				}, nil
			}),
			additionalMetricsCheckFunc: func(h *Handler) {
				v := promtest.ToFloat64(h.rejectedQueries.WithLabelValues(reasonTooManySamples, userID))
				assert.Equal(t, float64(1), v)
			},
			expectedStatusCode: http.StatusUnprocessableEntity,
		},
		{
			name:            "test handler with reasonTooLongRange",
			cfg:             HandlerConfig{QueryStatsEnabled: true},
			expectedMetrics: 6,
			roundTripperFunc: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusUnprocessableEntity,
					Body:       io.NopCloser(strings.NewReader(limitTimeRangeExceeded)),
				}, nil
			}),
			additionalMetricsCheckFunc: func(h *Handler) {
				v := promtest.ToFloat64(h.rejectedQueries.WithLabelValues(reasonTimeRangeExceeded, userID))
				assert.Equal(t, float64(1), v)
			},
			expectedStatusCode: http.StatusUnprocessableEntity,
		},
		{
			name:            "test handler with reasonSeriesFetched",
			cfg:             HandlerConfig{QueryStatsEnabled: true},
			expectedMetrics: 6,
			roundTripperFunc: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusUnprocessableEntity,
					Body:       io.NopCloser(strings.NewReader(limitSeriesFetched)),
				}, nil
			}),
			additionalMetricsCheckFunc: func(h *Handler) {
				v := promtest.ToFloat64(h.rejectedQueries.WithLabelValues(reasonSeriesFetched, userID))
				assert.Equal(t, float64(1), v)
			},
			expectedStatusCode: http.StatusUnprocessableEntity,
		},
		{
			name:            "test handler with reasonChunksFetched",
			cfg:             HandlerConfig{QueryStatsEnabled: true},
			expectedMetrics: 6,
			roundTripperFunc: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusUnprocessableEntity,
					Body:       io.NopCloser(strings.NewReader(limitChunksFetched)),
				}, nil
			}),
			additionalMetricsCheckFunc: func(h *Handler) {
				v := promtest.ToFloat64(h.rejectedQueries.WithLabelValues(reasonChunksFetched, userID))
				assert.Equal(t, float64(1), v)
			},
			expectedStatusCode: http.StatusUnprocessableEntity,
		},
		{
			name:            "test handler with reasonChunkBytesFetched",
			cfg:             HandlerConfig{QueryStatsEnabled: true},
			expectedMetrics: 6,
			roundTripperFunc: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusUnprocessableEntity,
					Body:       io.NopCloser(strings.NewReader(limitChunkBytesFetched)),
				}, nil
			}),
			additionalMetricsCheckFunc: func(h *Handler) {
				v := promtest.ToFloat64(h.rejectedQueries.WithLabelValues(reasonChunkBytesFetched, userID))
				assert.Equal(t, float64(1), v)
			},
			expectedStatusCode: http.StatusUnprocessableEntity,
		},
		{
			name:            "test handler with reasonDataBytesFetched",
			cfg:             HandlerConfig{QueryStatsEnabled: true},
			expectedMetrics: 6,
			roundTripperFunc: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusUnprocessableEntity,
					Body:       io.NopCloser(strings.NewReader(limitDataBytesFetched)),
				}, nil
			}),
			additionalMetricsCheckFunc: func(h *Handler) {
				v := promtest.ToFloat64(h.rejectedQueries.WithLabelValues(reasonDataBytesFetched, userID))
				assert.Equal(t, float64(1), v)
			},
			expectedStatusCode: http.StatusUnprocessableEntity,
		},
		{
			name:            "test handler with reasonSeriesLimitStoreGateway",
			cfg:             HandlerConfig{QueryStatsEnabled: true},
			expectedMetrics: 6,
			roundTripperFunc: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusUnprocessableEntity,
					Body:       io.NopCloser(strings.NewReader(limitSeriesStoreGateway)),
				}, nil
			}),
			additionalMetricsCheckFunc: func(h *Handler) {
				v := promtest.ToFloat64(h.rejectedQueries.WithLabelValues(reasonSeriesLimitStoreGateway, userID))
				assert.Equal(t, float64(1), v)
			},
			expectedStatusCode: http.StatusUnprocessableEntity,
		},
		{
			name:            "test handler with reasonChunksLimitStoreGateway",
			cfg:             HandlerConfig{QueryStatsEnabled: true},
			expectedMetrics: 6,
			roundTripperFunc: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusUnprocessableEntity,
					Body:       io.NopCloser(strings.NewReader(limitChunksStoreGateway)),
				}, nil
			}),
			additionalMetricsCheckFunc: func(h *Handler) {
				v := promtest.ToFloat64(h.rejectedQueries.WithLabelValues(reasonChunksLimitStoreGateway, userID))
				assert.Equal(t, float64(1), v)
			},
			expectedStatusCode: http.StatusUnprocessableEntity,
		},
		{
			name:            "test handler with reasonBytesLimitStoreGateway",
			cfg:             HandlerConfig{QueryStatsEnabled: true},
			expectedMetrics: 6,
			roundTripperFunc: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusUnprocessableEntity,
					Body:       io.NopCloser(strings.NewReader(limitBytesStoreGateway)),
				}, nil
			}),
			additionalMetricsCheckFunc: func(h *Handler) {
				v := promtest.ToFloat64(h.rejectedQueries.WithLabelValues(reasonBytesLimitStoreGateway, userID))
				assert.Equal(t, float64(1), v)
			},
			expectedStatusCode: http.StatusUnprocessableEntity,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			handler := NewHandler(tt.cfg, tt.roundTripperFunc, log.NewNopLogger(), reg)

			ctx := user.InjectOrgID(context.Background(), userID)
			req := httptest.NewRequest("GET", "/", nil)
			req = req.WithContext(ctx)
			resp := httptest.NewRecorder()

			handler.ServeHTTP(resp, req)
			_, _ = io.ReadAll(resp.Body)
			require.Equal(t, resp.Code, tt.expectedStatusCode)

			count, err := promtest.GatherAndCount(
				reg,
				"cortex_query_seconds_total",
				"cortex_query_fetched_series_total",
				"cortex_query_samples_total",
				"cortex_query_fetched_chunks_bytes_total",
				"cortex_query_samples_scanned_total",
				"cortex_query_peak_samples",
			)

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedMetrics, count)

			if tt.additionalMetricsCheckFunc != nil {
				tt.additionalMetricsCheckFunc(handler)
			}
		})
	}
}

func TestReportQueryStatsFormat(t *testing.T) {
	outputBuf := bytes.NewBuffer(nil)
	logger := log.NewSyncLogger(log.NewLogfmtLogger(outputBuf))
	handler := NewHandler(HandlerConfig{QueryStatsEnabled: true}, http.DefaultTransport, logger, nil)
	userID := "fake"
	req, _ := http.NewRequest(http.MethodGet, "http://localhost:8080/prometheus/api/v1/query", nil)
	resp := &http.Response{ContentLength: 1000}
	responseTime := time.Second
	statusCode := http.StatusOK

	type testCase struct {
		queryString url.Values
		queryStats  *querier_stats.QueryStats
		header      http.Header
		responseErr error
		expectedLog string
	}

	tests := map[string]testCase{
		"should not include query and header details if empty": {
			expectedLog: `level=info msg="query stats" component=query-frontend method=GET path=/prometheus/api/v1/query response_time=1s query_wall_time_seconds=0 response_series_count=0 fetched_series_count=0 fetched_chunks_count=0 fetched_samples_count=0 fetched_chunks_bytes=0 fetched_data_bytes=0 split_queries=0 status_code=200 response_size=1000`,
		},
		"should include query length and string at the end": {
			queryString: url.Values(map[string][]string{"query": {"up"}}),
			expectedLog: `level=info msg="query stats" component=query-frontend method=GET path=/prometheus/api/v1/query response_time=1s query_wall_time_seconds=0 response_series_count=0 fetched_series_count=0 fetched_chunks_count=0 fetched_samples_count=0 fetched_chunks_bytes=0 fetched_data_bytes=0 split_queries=0 status_code=200 response_size=1000 query_length=2 param_query=up`,
		},
		"should include query stats": {
			queryStats: &querier_stats.QueryStats{
				QueryResponseSeries: 100,
				Stats: querier_stats.Stats{
					WallTime:             3 * time.Second,
					QueryStorageWallTime: 100 * time.Minute,
					FetchedSeriesCount:   100,
					FetchedChunksCount:   200,
					FetchedSamplesCount:  300,
					FetchedChunkBytes:    1024,
					FetchedDataBytes:     2048,
					SplitQueries:         10,
				},
			},
			expectedLog: `level=info msg="query stats" component=query-frontend method=GET path=/prometheus/api/v1/query response_time=1s query_wall_time_seconds=3 response_series_count=100 fetched_series_count=100 fetched_chunks_count=200 fetched_samples_count=300 fetched_chunks_bytes=1024 fetched_data_bytes=2048 split_queries=10 status_code=200 response_size=1000 query_storage_wall_time_seconds=6000`,
		},
		"should include user agent": {
			header:      http.Header{"User-Agent": []string{"Grafana"}},
			expectedLog: `level=info msg="query stats" component=query-frontend method=GET path=/prometheus/api/v1/query response_time=1s query_wall_time_seconds=0 response_series_count=0 fetched_series_count=0 fetched_chunks_count=0 fetched_samples_count=0 fetched_chunks_bytes=0 fetched_data_bytes=0 split_queries=0 status_code=200 response_size=1000 user_agent=Grafana`,
		},
		"should include response error": {
			responseErr: errors.New("foo_err"),
			expectedLog: `level=error msg="query stats" component=query-frontend method=GET path=/prometheus/api/v1/query response_time=1s query_wall_time_seconds=0 response_series_count=0 fetched_series_count=0 fetched_chunks_count=0 fetched_samples_count=0 fetched_chunks_bytes=0 fetched_data_bytes=0 split_queries=0 status_code=200 response_size=1000 error=foo_err`,
		},
		"should include query priority": {
			queryString: url.Values(map[string][]string{"query": {"up"}}),
			queryStats: &querier_stats.QueryStats{
				Priority:         99,
				PriorityAssigned: true,
			},
			expectedLog: `level=info msg="query stats" component=query-frontend method=GET path=/prometheus/api/v1/query response_time=1s query_wall_time_seconds=0 response_series_count=0 fetched_series_count=0 fetched_chunks_count=0 fetched_samples_count=0 fetched_chunks_bytes=0 fetched_data_bytes=0 split_queries=0 status_code=200 response_size=1000 query_length=2 priority=99 param_query=up`,
		},
		"should include data fetch min and max time": {
			queryString: url.Values(map[string][]string{"query": {"up"}}),
			queryStats: &querier_stats.QueryStats{
				DataSelectMaxTime: 1704153600000,
				DataSelectMinTime: 1704067200000,
			},
			expectedLog: `level=info msg="query stats" component=query-frontend method=GET path=/prometheus/api/v1/query response_time=1s query_wall_time_seconds=0 response_series_count=0 fetched_series_count=0 fetched_chunks_count=0 fetched_samples_count=0 fetched_chunks_bytes=0 fetched_data_bytes=0 split_queries=0 status_code=200 response_size=1000 data_select_max_time=1704153600 data_select_min_time=1704067200 query_length=2 param_query=up`,
		},
		"should include query stats with store gateway stats": {
			queryStats: &querier_stats.QueryStats{
				QueryResponseSeries: 100,
				Stats: querier_stats.Stats{
					WallTime:                         3 * time.Second,
					QueryStorageWallTime:             100 * time.Minute,
					FetchedSeriesCount:               100,
					FetchedChunksCount:               200,
					FetchedSamplesCount:              300,
					FetchedChunkBytes:                1024,
					FetchedDataBytes:                 2048,
					SplitQueries:                     10,
					StoreGatewayTouchedPostingsCount: 20,
					StoreGatewayTouchedPostingBytes:  200,
				},
			},
			expectedLog: `level=info msg="query stats" component=query-frontend method=GET path=/prometheus/api/v1/query response_time=1s query_wall_time_seconds=3 response_series_count=100 fetched_series_count=100 fetched_chunks_count=200 fetched_samples_count=300 fetched_chunks_bytes=1024 fetched_data_bytes=2048 split_queries=10 status_code=200 response_size=1000 store_gateway_touched_postings_count=20 store_gateway_touched_posting_bytes=200 query_storage_wall_time_seconds=6000`,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			req.Header = testData.header
			handler.reportQueryStats(req, userID, testData.queryString, responseTime, testData.queryStats, testData.responseErr, statusCode, resp)
			data, err := io.ReadAll(outputBuf)
			require.NoError(t, err)
			require.Equal(t, testData.expectedLog+"\n", string(data))
		})
	}
}
