package transport

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestWriteError(t *testing.T) {
	for _, test := range []struct {
		status int
		err    error
	}{
		{http.StatusInternalServerError, errors.New("unknown")},
		{http.StatusGatewayTimeout, context.DeadlineExceeded},
		{StatusClientClosedRequest, context.Canceled},
		{http.StatusBadRequest, httpgrpc.Errorf(http.StatusBadRequest, "")},
		{http.StatusRequestEntityTooLarge, errors.New("http: request body too large")},
	} {
		t.Run(test.err.Error(), func(t *testing.T) {
			w := httptest.NewRecorder()
			writeError(w, test.err)
			require.Equal(t, test.status, w.Result().StatusCode)
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
		roundTripperFunc           roundTripperFunc
		additionalMetricsCheckFunc func(h *Handler)
	}{
		{
			name:             "test handler with stats enabled",
			cfg:              HandlerConfig{QueryStatsEnabled: true},
			expectedMetrics:  3,
			roundTripperFunc: roundTripper,
		},
		{
			name:             "test handler with stats disabled",
			cfg:              HandlerConfig{QueryStatsEnabled: false},
			expectedMetrics:  0,
			roundTripperFunc: roundTripper,
		},
		{
			name:            "test handler with reasonResponseTooLarge",
			cfg:             HandlerConfig{QueryStatsEnabled: true},
			expectedMetrics: 3,
			roundTripperFunc: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusRequestEntityTooLarge,
					Body:       io.NopCloser(strings.NewReader("{}")),
				}, nil
			}),
			additionalMetricsCheckFunc: func(h *Handler) {
				v := promtest.ToFloat64(h.rejectedQueries.WithLabelValues(reasonRequestBodySizeExceeded, userID))
				assert.Equal(t, float64(1), v)
			},
		},
		{
			name:            "test handler with reasonTooManyRequests",
			cfg:             HandlerConfig{QueryStatsEnabled: true},
			expectedMetrics: 3,
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
		},
		{
			name:            "test handler with reasonTooManySamples",
			cfg:             HandlerConfig{QueryStatsEnabled: true},
			expectedMetrics: 3,
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
		},
		{
			name:            "test handler with reasonTooLongRange",
			cfg:             HandlerConfig{QueryStatsEnabled: true},
			expectedMetrics: 3,
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
		},
		{
			name:            "test handler with reasonSeriesFetched",
			cfg:             HandlerConfig{QueryStatsEnabled: true},
			expectedMetrics: 3,
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
		},
		{
			name:            "test handler with reasonChunksFetched",
			cfg:             HandlerConfig{QueryStatsEnabled: true},
			expectedMetrics: 3,
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
		},
		{
			name:            "test handler with reasonChunkBytesFetched",
			cfg:             HandlerConfig{QueryStatsEnabled: true},
			expectedMetrics: 3,
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
		},
		{
			name:            "test handler with reasonDataBytesFetched",
			cfg:             HandlerConfig{QueryStatsEnabled: true},
			expectedMetrics: 3,
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
		},
		{
			name:            "test handler with reasonSeriesLimitStoreGateway",
			cfg:             HandlerConfig{QueryStatsEnabled: true},
			expectedMetrics: 3,
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
		},
		{
			name:            "test handler with reasonChunksLimitStoreGateway",
			cfg:             HandlerConfig{QueryStatsEnabled: true},
			expectedMetrics: 3,
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
		},
		{
			name:            "test handler with reasonBytesLimitStoreGateway",
			cfg:             HandlerConfig{QueryStatsEnabled: true},
			expectedMetrics: 3,
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
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			handler := NewHandler(tt.cfg, roundTripper, log.NewNopLogger(), reg)

			ctx := user.InjectOrgID(context.Background(), userID)
			req := httptest.NewRequest("GET", "/", nil)
			req = req.WithContext(ctx)
			resp := httptest.NewRecorder()

			handler.ServeHTTP(resp, req)
			_, _ = io.ReadAll(resp.Body)
			require.Equal(t, resp.Code, http.StatusOK)

			count, err := promtest.GatherAndCount(
				reg,
				"cortex_query_seconds_total",
				"cortex_query_fetched_series_total",
				"cortex_query_fetched_chunks_bytes_total",
			)

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedMetrics, count)
		})
	}
}
