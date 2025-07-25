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
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc/codes"

	querier_stats "github.com/cortexproject/cortex/pkg/querier/stats"
	"github.com/cortexproject/cortex/pkg/querier/tenantfederation"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/tenant"
	util_api "github.com/cortexproject/cortex/pkg/util/api"
	"github.com/cortexproject/cortex/pkg/util/limiter"
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
	tenantFederationCfg := tenantfederation.Config{}
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
				v := promtest.ToFloat64(h.rejectedQueries.WithLabelValues(reasonResponseBodySizeExceeded, tripperware.SourceAPI, userID))
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
				v := promtest.ToFloat64(h.rejectedQueries.WithLabelValues(reasonTooManyRequests, tripperware.SourceAPI, userID))
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
				v := promtest.ToFloat64(h.rejectedQueries.WithLabelValues(reasonTooManySamples, tripperware.SourceAPI, userID))
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
				v := promtest.ToFloat64(h.rejectedQueries.WithLabelValues(reasonTimeRangeExceeded, tripperware.SourceAPI, userID))
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
				v := promtest.ToFloat64(h.rejectedQueries.WithLabelValues(reasonSeriesFetched, tripperware.SourceAPI, userID))
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
				v := promtest.ToFloat64(h.rejectedQueries.WithLabelValues(reasonChunksFetched, tripperware.SourceAPI, userID))
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
				v := promtest.ToFloat64(h.rejectedQueries.WithLabelValues(reasonChunkBytesFetched, tripperware.SourceAPI, userID))
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
				v := promtest.ToFloat64(h.rejectedQueries.WithLabelValues(reasonDataBytesFetched, tripperware.SourceAPI, userID))
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
				v := promtest.ToFloat64(h.rejectedQueries.WithLabelValues(reasonSeriesLimitStoreGateway, tripperware.SourceAPI, userID))
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
				v := promtest.ToFloat64(h.rejectedQueries.WithLabelValues(reasonChunksLimitStoreGateway, tripperware.SourceAPI, userID))
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
				v := promtest.ToFloat64(h.rejectedQueries.WithLabelValues(reasonBytesLimitStoreGateway, tripperware.SourceAPI, userID))
				assert.Equal(t, float64(1), v)
			},
			expectedStatusCode: http.StatusUnprocessableEntity,
		},
		{
			name:            "test handler with reasonResourceExhausted",
			cfg:             HandlerConfig{QueryStatsEnabled: true},
			expectedMetrics: 6,
			roundTripperFunc: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				resourceLimitReachedErr := &limiter.ResourceLimitReachedError{}
				return &http.Response{
					StatusCode: http.StatusUnprocessableEntity,
					Body:       io.NopCloser(strings.NewReader(resourceLimitReachedErr.Error())),
				}, nil
			}),
			additionalMetricsCheckFunc: func(h *Handler) {
				v := promtest.ToFloat64(h.rejectedQueries.WithLabelValues(reasonResourceExhausted, tripperware.SourceAPI, userID))
				assert.Equal(t, float64(1), v)
			},
			expectedStatusCode: http.StatusUnprocessableEntity,
		},
		{
			name:            "test cortex_slow_queries_total",
			cfg:             HandlerConfig{QueryStatsEnabled: true, LogQueriesLongerThan: time.Second * 2},
			expectedMetrics: 7,
			roundTripperFunc: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				time.Sleep(time.Second * 4)
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("mock")),
				}, nil
			}),
			additionalMetricsCheckFunc: func(h *Handler) {
				v := promtest.ToFloat64(h.slowQueries.WithLabelValues(tripperware.SourceAPI, userID))
				assert.Equal(t, float64(1), v)
			},
			expectedStatusCode: http.StatusOK,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			handler := NewHandler(tt.cfg, tenantFederationCfg, tt.roundTripperFunc, log.NewNopLogger(), reg)

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
				"cortex_slow_queries_total",
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
	userID := "fake"
	req, _ := http.NewRequest(http.MethodGet, "http://localhost:8080/prometheus/api/v1/query", nil)
	resp := &http.Response{ContentLength: 1000}
	responseTime := time.Second
	statusCode := http.StatusOK

	type testCase struct {
		queryString               url.Values
		queryStats                *querier_stats.QueryStats
		header                    http.Header
		responseErr               error
		expectedLog               string
		enabledRulerQueryStatsLog bool
		source                    string
	}

	tests := map[string]testCase{
		"should not include query and header details if empty": {
			expectedLog: `level=info msg="query stats" component=query-frontend method=GET path=/prometheus/api/v1/query response_time=1s query_wall_time_seconds=0 response_series_count=0 fetched_series_count=0 fetched_chunks_count=0 fetched_samples_count=0 fetched_chunks_bytes=0 fetched_data_bytes=0 split_queries=0 status_code=200 response_size=1000 samples_scanned=0`,
			source:      tripperware.SourceAPI,
		},
		"should include query length and string at the end": {
			queryString: url.Values(map[string][]string{"query": {"up"}}),
			expectedLog: `level=info msg="query stats" component=query-frontend method=GET path=/prometheus/api/v1/query response_time=1s query_wall_time_seconds=0 response_series_count=0 fetched_series_count=0 fetched_chunks_count=0 fetched_samples_count=0 fetched_chunks_bytes=0 fetched_data_bytes=0 split_queries=0 status_code=200 response_size=1000 samples_scanned=0 query_length=2 param_query=up`,
			source:      tripperware.SourceAPI,
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
			expectedLog: `level=info msg="query stats" component=query-frontend method=GET path=/prometheus/api/v1/query response_time=1s query_wall_time_seconds=3 response_series_count=100 fetched_series_count=100 fetched_chunks_count=200 fetched_samples_count=300 fetched_chunks_bytes=1024 fetched_data_bytes=2048 split_queries=10 status_code=200 response_size=1000 samples_scanned=0 query_storage_wall_time_seconds=6000`,
			source:      tripperware.SourceAPI,
		},
		"should include user agent": {
			header:      http.Header{"User-Agent": []string{"Grafana"}},
			expectedLog: `level=info msg="query stats" component=query-frontend method=GET path=/prometheus/api/v1/query response_time=1s query_wall_time_seconds=0 response_series_count=0 fetched_series_count=0 fetched_chunks_count=0 fetched_samples_count=0 fetched_chunks_bytes=0 fetched_data_bytes=0 split_queries=0 status_code=200 response_size=1000 samples_scanned=0 user_agent=Grafana`,
			source:      tripperware.SourceAPI,
		},
		"should include response error": {
			responseErr: errors.New("foo_err"),
			expectedLog: `level=error msg="query stats" component=query-frontend method=GET path=/prometheus/api/v1/query response_time=1s query_wall_time_seconds=0 response_series_count=0 fetched_series_count=0 fetched_chunks_count=0 fetched_samples_count=0 fetched_chunks_bytes=0 fetched_data_bytes=0 split_queries=0 status_code=200 response_size=1000 samples_scanned=0 error=foo_err`,
			source:      tripperware.SourceAPI,
		},
		"should include query priority": {
			queryString: url.Values(map[string][]string{"query": {"up"}}),
			queryStats: &querier_stats.QueryStats{
				Priority:         99,
				PriorityAssigned: true,
			},
			expectedLog: `level=info msg="query stats" component=query-frontend method=GET path=/prometheus/api/v1/query response_time=1s query_wall_time_seconds=0 response_series_count=0 fetched_series_count=0 fetched_chunks_count=0 fetched_samples_count=0 fetched_chunks_bytes=0 fetched_data_bytes=0 split_queries=0 status_code=200 response_size=1000 samples_scanned=0 query_length=2 priority=99 param_query=up`,
			source:      tripperware.SourceAPI,
		},
		"should include data fetch min and max time": {
			queryString: url.Values(map[string][]string{"query": {"up"}}),
			queryStats: &querier_stats.QueryStats{
				DataSelectMaxTime: 1704153600000,
				DataSelectMinTime: 1704067200000,
			},
			expectedLog: `level=info msg="query stats" component=query-frontend method=GET path=/prometheus/api/v1/query response_time=1s query_wall_time_seconds=0 response_series_count=0 fetched_series_count=0 fetched_chunks_count=0 fetched_samples_count=0 fetched_chunks_bytes=0 fetched_data_bytes=0 split_queries=0 status_code=200 response_size=1000 samples_scanned=0 data_select_max_time=1704153600 data_select_min_time=1704067200 query_length=2 param_query=up`,
			source:      tripperware.SourceAPI,
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
			expectedLog: `level=info msg="query stats" component=query-frontend method=GET path=/prometheus/api/v1/query response_time=1s query_wall_time_seconds=3 response_series_count=100 fetched_series_count=100 fetched_chunks_count=200 fetched_samples_count=300 fetched_chunks_bytes=1024 fetched_data_bytes=2048 split_queries=10 status_code=200 response_size=1000 samples_scanned=0 store_gateway_touched_postings_count=20 store_gateway_touched_posting_bytes=200 query_storage_wall_time_seconds=6000`,
			source:      tripperware.SourceAPI,
		},
		"should not report a log": {
			expectedLog:               ``,
			source:                    tripperware.SourceRuler,
			enabledRulerQueryStatsLog: false,
		},
		"should report a log": {
			expectedLog:               `level=info msg="query stats" component=query-frontend method=GET path=/prometheus/api/v1/query response_time=1s query_wall_time_seconds=0 response_series_count=0 fetched_series_count=0 fetched_chunks_count=0 fetched_samples_count=0 fetched_chunks_bytes=0 fetched_data_bytes=0 split_queries=0 status_code=200 response_size=1000 samples_scanned=0`,
			source:                    tripperware.SourceRuler,
			enabledRulerQueryStatsLog: true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			handler := NewHandler(HandlerConfig{QueryStatsEnabled: true, EnabledRulerQueryStatsLog: testData.enabledRulerQueryStatsLog}, tenantfederation.Config{}, http.DefaultTransport, logger, nil)
			req.Header = testData.header
			handler.reportQueryStats(req, testData.source, userID, testData.queryString, responseTime, testData.queryStats, testData.responseErr, statusCode, resp)
			data, err := io.ReadAll(outputBuf)
			require.NoError(t, err)
			if testData.expectedLog == "" {
				require.Empty(t, string(data))
			} else {
				require.Equal(t, testData.expectedLog+"\n", string(data))
			}
		})
	}
}

func Test_ExtractTenantIDs(t *testing.T) {
	roundTripper := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("{}")),
		}, nil
	})

	tests := []struct {
		name               string
		orgId              string
		expectedStatusCode int
	}{
		{
			name:               "invalid tenantID",
			orgId:              "aaa\\/",
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name:               "valid tenantID",
			orgId:              "user-1",
			expectedStatusCode: http.StatusOK,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := NewHandler(HandlerConfig{QueryStatsEnabled: true}, tenantfederation.Config{}, roundTripper, log.NewNopLogger(), nil)
			handlerWithAuth := middleware.Merge(middleware.AuthenticateUser).Wrap(handler)

			req := httptest.NewRequest("GET", "http://fake", nil)
			req.Header.Set("X-Scope-OrgId", test.orgId)
			resp := httptest.NewRecorder()

			handlerWithAuth.ServeHTTP(resp, req)
			require.Equal(t, test.expectedStatusCode, resp.Code)
		})
	}
}

func Test_TenantFederation_MaxTenant(t *testing.T) {
	// set a multi tenant resolver
	tenant.WithDefaultResolver(tenant.NewMultiResolver())

	roundTripper := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("{}")),
		}, nil
	})

	tests := []struct {
		name               string
		cfg                tenantfederation.Config
		orgId              string
		expectedStatusCode int
		expectedErrMsg     string
	}{
		{
			name: "one tenant",
			cfg: tenantfederation.Config{
				Enabled:   true,
				MaxTenant: 0,
			},
			orgId:              "org1",
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "less than max tenant",
			cfg: tenantfederation.Config{
				Enabled:   true,
				MaxTenant: 3,
			},
			orgId:              "org1|org2",
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "equal to max tenant",
			cfg: tenantfederation.Config{
				Enabled:   true,
				MaxTenant: 2,
			},
			orgId:              "org1|org2",
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "exceeds max tenant",
			cfg: tenantfederation.Config{
				Enabled:   true,
				MaxTenant: 2,
			},
			orgId:              "org1|org2|org3",
			expectedStatusCode: http.StatusBadRequest,
			expectedErrMsg:     "too many tenants, max: 2, actual: 3",
		},
		{
			name: "no org Id",
			cfg: tenantfederation.Config{
				Enabled:   true,
				MaxTenant: 0,
			},
			orgId:              "",
			expectedStatusCode: http.StatusUnauthorized,
			expectedErrMsg:     "no org id",
		},
		{
			name: "no limit",
			cfg: tenantfederation.Config{
				Enabled:   true,
				MaxTenant: 0,
			},
			orgId:              "org1|org2|org3",
			expectedStatusCode: http.StatusOK,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := NewHandler(HandlerConfig{QueryStatsEnabled: true}, test.cfg, roundTripper, log.NewNopLogger(), nil)
			handlerWithAuth := middleware.Merge(middleware.AuthenticateUser).Wrap(handler)

			req := httptest.NewRequest("GET", "http://fake", nil)
			req.Header.Set("X-Scope-OrgId", test.orgId)
			resp := httptest.NewRecorder()

			handlerWithAuth.ServeHTTP(resp, req)

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, test.expectedStatusCode, resp.Code)

			if test.expectedErrMsg != "" {
				require.Contains(t, string(body), test.expectedErrMsg)

				if strings.Contains(test.expectedErrMsg, "too many tenants") {
					v := promtest.ToFloat64(handler.rejectedQueries.WithLabelValues(reasonTooManyTenants, tripperware.SourceAPI, test.orgId))
					assert.Equal(t, float64(1), v)
				}
			}
		})
	}
}

func TestHandlerMetricsCleanup(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	handler := NewHandler(HandlerConfig{QueryStatsEnabled: true}, tenantfederation.Config{}, http.DefaultTransport, log.NewNopLogger(), reg)

	user1 := "user1"
	user2 := "user2"
	source := "api"

	// Simulate activity for user1
	handler.querySeconds.WithLabelValues(source, user1).Add(1.0)
	handler.queryFetchedSeries.WithLabelValues(source, user1).Add(100)
	handler.queryFetchedSamples.WithLabelValues(source, user1).Add(1000)
	handler.queryScannedSamples.WithLabelValues(source, user1).Add(2000)
	handler.queryPeakSamples.WithLabelValues(source, user1).Observe(500)
	handler.queryChunkBytes.WithLabelValues(source, user1).Add(1024)
	handler.queryDataBytes.WithLabelValues(source, user1).Add(2048)
	handler.rejectedQueries.WithLabelValues(reasonTooManySamples, source, user1).Add(5)
	handler.getOrCreateSlowQueryMetric().WithLabelValues(source, user1).Add(5)

	// Simulate activity for user2
	handler.querySeconds.WithLabelValues(source, user2).Add(2.0)
	handler.queryFetchedSeries.WithLabelValues(source, user2).Add(200)
	handler.queryFetchedSamples.WithLabelValues(source, user2).Add(2000)
	handler.queryScannedSamples.WithLabelValues(source, user2).Add(4000)
	handler.queryPeakSamples.WithLabelValues(source, user2).Observe(1000)
	handler.queryChunkBytes.WithLabelValues(source, user2).Add(2048)
	handler.queryDataBytes.WithLabelValues(source, user2).Add(4096)
	handler.rejectedQueries.WithLabelValues(reasonTooManySamples, source, user2).Add(10)
	handler.getOrCreateSlowQueryMetric().WithLabelValues(source, user2).Add(10)

	// Verify initial state - both users should have metrics
	require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_seconds_total Total amount of wall clock time spend processing queries.
		# TYPE cortex_query_seconds_total counter
		cortex_query_seconds_total{source="api",user="user1"} 1
		cortex_query_seconds_total{source="api",user="user2"} 2
		# HELP cortex_query_fetched_series_total Number of series fetched to execute a query.
		# TYPE cortex_query_fetched_series_total counter
		cortex_query_fetched_series_total{source="api",user="user1"} 100
		cortex_query_fetched_series_total{source="api",user="user2"} 200
		# HELP cortex_query_samples_total Number of samples fetched to execute a query.
		# TYPE cortex_query_samples_total counter
		cortex_query_samples_total{source="api",user="user1"} 1000
		cortex_query_samples_total{source="api",user="user2"} 2000
		# HELP cortex_query_samples_scanned_total Number of samples scanned to execute a query.
		# TYPE cortex_query_samples_scanned_total counter
		cortex_query_samples_scanned_total{source="api",user="user1"} 2000
		cortex_query_samples_scanned_total{source="api",user="user2"} 4000
		# HELP cortex_query_peak_samples Highest count of samples considered to execute a query.
		# TYPE cortex_query_peak_samples histogram
		cortex_query_peak_samples_bucket{source="api",user="user1",le="+Inf"} 1
		cortex_query_peak_samples_sum{source="api",user="user1"} 500
		cortex_query_peak_samples_count{source="api",user="user1"} 1
		cortex_query_peak_samples_bucket{source="api",user="user2",le="+Inf"} 1
		cortex_query_peak_samples_sum{source="api",user="user2"} 1000
		cortex_query_peak_samples_count{source="api",user="user2"} 1
		# HELP cortex_query_fetched_chunks_bytes_total Size of all chunks fetched to execute a query in bytes.
		# TYPE cortex_query_fetched_chunks_bytes_total counter
		cortex_query_fetched_chunks_bytes_total{source="api",user="user1"} 1024
		cortex_query_fetched_chunks_bytes_total{source="api",user="user2"} 2048
		# HELP cortex_query_fetched_data_bytes_total Size of all data fetched to execute a query in bytes.
		# TYPE cortex_query_fetched_data_bytes_total counter
		cortex_query_fetched_data_bytes_total{source="api",user="user1"} 2048
		cortex_query_fetched_data_bytes_total{source="api",user="user2"} 4096
		# HELP cortex_rejected_queries_total The total number of queries that were rejected.
		# TYPE cortex_rejected_queries_total counter
		cortex_rejected_queries_total{reason="too_many_samples",source="api",user="user1"} 5
		cortex_rejected_queries_total{reason="too_many_samples",source="api",user="user2"} 10
		# HELP cortex_slow_queries_total The total number of slow queries.
		# TYPE cortex_slow_queries_total counter
		cortex_slow_queries_total{source="api",user="user1"} 5
		cortex_slow_queries_total{source="api",user="user2"} 10
	`), "cortex_query_seconds_total", "cortex_query_fetched_series_total", "cortex_query_samples_total",
		"cortex_query_samples_scanned_total", "cortex_query_peak_samples", "cortex_query_fetched_chunks_bytes_total",
		"cortex_query_fetched_data_bytes_total", "cortex_rejected_queries_total", "cortex_slow_queries_total"))

	// Clean up metrics for user1
	handler.cleanupMetricsForInactiveUser(user1)

	// Verify final state - only user2 should have metrics
	require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_seconds_total Total amount of wall clock time spend processing queries.
		# TYPE cortex_query_seconds_total counter
		cortex_query_seconds_total{source="api",user="user2"} 2
		# HELP cortex_query_fetched_series_total Number of series fetched to execute a query.
		# TYPE cortex_query_fetched_series_total counter
		cortex_query_fetched_series_total{source="api",user="user2"} 200
		# HELP cortex_query_samples_total Number of samples fetched to execute a query.
		# TYPE cortex_query_samples_total counter
		cortex_query_samples_total{source="api",user="user2"} 2000
		# HELP cortex_query_samples_scanned_total Number of samples scanned to execute a query.
		# TYPE cortex_query_samples_scanned_total counter
		cortex_query_samples_scanned_total{source="api",user="user2"} 4000
		# HELP cortex_query_peak_samples Highest count of samples considered to execute a query.
		# TYPE cortex_query_peak_samples histogram
		cortex_query_peak_samples_bucket{source="api",user="user2",le="+Inf"} 1
		cortex_query_peak_samples_sum{source="api",user="user2"} 1000
		cortex_query_peak_samples_count{source="api",user="user2"} 1
		# HELP cortex_query_fetched_chunks_bytes_total Size of all chunks fetched to execute a query in bytes.
		# TYPE cortex_query_fetched_chunks_bytes_total counter
		cortex_query_fetched_chunks_bytes_total{source="api",user="user2"} 2048
		# HELP cortex_query_fetched_data_bytes_total Size of all data fetched to execute a query in bytes.
		# TYPE cortex_query_fetched_data_bytes_total counter
		cortex_query_fetched_data_bytes_total{source="api",user="user2"} 4096
		# HELP cortex_rejected_queries_total The total number of queries that were rejected.
		# TYPE cortex_rejected_queries_total counter
		cortex_rejected_queries_total{reason="too_many_samples",source="api",user="user2"} 10
		# HELP cortex_slow_queries_total The total number of slow queries.
		# TYPE cortex_slow_queries_total counter
		cortex_slow_queries_total{source="api",user="user2"} 10
	`), "cortex_query_seconds_total", "cortex_query_fetched_series_total", "cortex_query_samples_total",
		"cortex_query_samples_scanned_total", "cortex_query_peak_samples", "cortex_query_fetched_chunks_bytes_total",
		"cortex_query_fetched_data_bytes_total", "cortex_rejected_queries_total", "cortex_slow_queries_total"))
}
