package querier

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func TestCardinalityHandler_ParameterValidation(t *testing.T) {
	limits := validation.Limits{}
	flagext.DefaultValues(&limits)
	limits.CardinalityAPIEnabled = true
	overrides := validation.NewOverrides(limits, nil)

	dist := &MockDistributor{}
	dist.On("Cardinality", mock.Anything, mock.Anything).Return(&client.CardinalityResponse{
		NumSeries: 100,
	}, nil).Maybe()

	handler := CardinalityHandler(dist, nil, overrides, prometheus.NewRegistry())

	tests := []struct {
		name          string
		query         string
		expectedCode  int
		expectedError string
	}{
		{
			name:         "default parameters",
			query:        "",
			expectedCode: http.StatusOK,
		},
		{
			name:          "invalid source",
			query:         "source=invalid",
			expectedCode:  http.StatusBadRequest,
			expectedError: `invalid source: must be "head" or "blocks"`,
		},
		{
			name:          "limit too low",
			query:         "limit=0",
			expectedCode:  http.StatusBadRequest,
			expectedError: "invalid limit: must be an integer between 1 and 512",
		},
		{
			name:          "limit too high",
			query:         "limit=513",
			expectedCode:  http.StatusBadRequest,
			expectedError: "invalid limit: must be an integer between 1 and 512",
		},
		{
			name:          "limit non-integer",
			query:         "limit=abc",
			expectedCode:  http.StatusBadRequest,
			expectedError: "invalid limit: must be an integer between 1 and 512",
		},
		{
			name:          "start with head source",
			query:         "source=head&start=1234567890",
			expectedCode:  http.StatusBadRequest,
			expectedError: "start and end parameters are not supported for source=head",
		},
		{
			name:          "end with head source",
			query:         "source=head&end=1234567890",
			expectedCode:  http.StatusBadRequest,
			expectedError: "start and end parameters are not supported for source=head",
		},
		{
			name:          "blocks without start/end",
			query:         "source=blocks",
			expectedCode:  http.StatusBadRequest,
			expectedError: "start and end are required for source=blocks",
		},
		{
			name:          "blocks with only start",
			query:         "source=blocks&start=1234567890",
			expectedCode:  http.StatusBadRequest,
			expectedError: "start and end are required for source=blocks",
		},
		{
			name:          "blocks with invalid start",
			query:         "source=blocks&start=invalid&end=1234567890",
			expectedCode:  http.StatusBadRequest,
			expectedError: "invalid start/end: must be RFC3339 or Unix timestamp",
		},
		{
			name:          "blocks with start >= end",
			query:         "source=blocks&start=1234567890&end=1234567890",
			expectedCode:  http.StatusBadRequest,
			expectedError: "invalid time range: start must be before end",
		},
		{
			name:         "valid limit",
			query:        "limit=50",
			expectedCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/api/v1/cardinality?"+tc.query, nil)
			req = req.WithContext(user.InjectOrgID(req.Context(), "test-tenant"))
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			assert.Equal(t, tc.expectedCode, rec.Code, "status code mismatch for %s", tc.name)

			if tc.expectedError != "" {
				var errResp cardinalityErrorResponse
				err := json.Unmarshal(rec.Body.Bytes(), &errResp)
				require.NoError(t, err)
				assert.Equal(t, statusError, errResp.Status)
				assert.Equal(t, tc.expectedError, errResp.Error)
			}
		})
	}
}

func TestCardinalityHandler_DisabledTenant(t *testing.T) {
	limits := validation.Limits{}
	flagext.DefaultValues(&limits)
	limits.CardinalityAPIEnabled = false
	overrides := validation.NewOverrides(limits, nil)

	dist := &MockDistributor{}
	handler := CardinalityHandler(dist, nil, overrides, prometheus.NewRegistry())

	req := httptest.NewRequest("GET", "/api/v1/cardinality", nil)
	req = req.WithContext(user.InjectOrgID(req.Context(), "test-tenant"))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Code)
}

func TestCardinalityHandler_SuccessfulResponse(t *testing.T) {
	limits := validation.Limits{}
	flagext.DefaultValues(&limits)
	limits.CardinalityAPIEnabled = true
	overrides := validation.NewOverrides(limits, nil)

	dist := &MockDistributor{}
	dist.On("Cardinality", mock.Anything, &client.CardinalityRequest{Limit: 10}).Return(&client.CardinalityResponse{
		NumSeries: 1500,
		SeriesCountByMetricName: []*cortexpb.CardinalityStatItem{
			{Name: "http_requests_total", Value: 500},
			{Name: "process_cpu_seconds_total", Value: 200},
		},
		LabelValueCountByLabelName: []*cortexpb.CardinalityStatItem{
			{Name: "instance", Value: 50},
			{Name: "job", Value: 10},
		},
		SeriesCountByLabelValuePair: []*cortexpb.CardinalityStatItem{
			{Name: "job=api-server", Value: 300},
			{Name: "instance=host1:9090", Value: 150},
		},
	}, nil)

	handler := CardinalityHandler(dist, nil, overrides, prometheus.NewRegistry())

	req := httptest.NewRequest("GET", "/api/v1/cardinality", nil)
	req = req.WithContext(user.InjectOrgID(req.Context(), "test-tenant"))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp cardinalityResponse
	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.Equal(t, statusSuccess, resp.Status)
	assert.Equal(t, uint64(1500), resp.Data.NumSeries)
	assert.False(t, resp.Data.Approximated)
	assert.Equal(t, 2, len(resp.Data.SeriesCountByMetricName))
	assert.Equal(t, "http_requests_total", resp.Data.SeriesCountByMetricName[0].Name)
	assert.Equal(t, uint64(500), resp.Data.SeriesCountByMetricName[0].Value)
	assert.Equal(t, 2, len(resp.Data.LabelValueCountByLabelName))
	assert.Equal(t, 2, len(resp.Data.SeriesCountByLabelValuePair))
}

func TestCardinalityHandler_ConcurrencyLimit(t *testing.T) {
	limits := validation.Limits{}
	flagext.DefaultValues(&limits)
	limits.CardinalityAPIEnabled = true
	limits.CardinalityMaxConcurrentRequests = 1
	overrides := validation.NewOverrides(limits, nil)

	// Create a limiter and pre-fill it
	limiter := newCardinalityConcurrencyLimiter(overrides)
	assert.True(t, limiter.tryAcquire("test-tenant"))
	assert.False(t, limiter.tryAcquire("test-tenant"))
	limiter.release("test-tenant")
	assert.True(t, limiter.tryAcquire("test-tenant"))
	limiter.release("test-tenant")
}
