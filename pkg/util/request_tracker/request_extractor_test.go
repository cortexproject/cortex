package request_tracker

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/util/requestmeta"
)

func TestDefaultExtractor(t *testing.T) {
	extractor := &DefaultExtractor{}
	req := httptest.NewRequest("GET", "/api/v1/test", nil)
	req.Header.Set("User-Agent", "test-agent")

	ctx := user.InjectOrgID(context.Background(), "test-tenant")
	ctx = requestmeta.ContextWithRequestId(ctx, "req-123")
	req = req.WithContext(ctx)

	result := extractor.Extract(req)
	require.NotEmpty(t, result)

	var data map[string]interface{}
	require.NoError(t, json.Unmarshal(result, &data))

	assert.Equal(t, "/api/v1/test", data["Path"])
	assert.Equal(t, "GET", data["Method"])
	assert.Equal(t, "test-tenant", data["TenantID"])
	assert.Equal(t, "req-123", data["RequestID"])
}

func TestGetSeriesExtractor(t *testing.T) {
	extractor := &ApiExtractor{}
	req := httptest.NewRequest("GET", "/api/v1/series", nil)
	q := req.URL.Query()
	q.Add("limit", "100")
	q.Add("match[]", "up")
	q.Add("match[]", "down")
	req.URL.RawQuery = q.Encode()

	result := extractor.Extract(req)
	require.NotEmpty(t, result)

	var data map[string]interface{}
	require.NoError(t, json.Unmarshal(result, &data))

	assert.Equal(t, "100", data["limit"])
	assert.Equal(t, float64(2), data["numberOfMatches"])
	assert.Contains(t, data["matches"], "up")
}

func TestInstantQueryExtractor(t *testing.T) {
	extractor := &InstantQueryExtractor{}
	req := httptest.NewRequest("GET", "/api/v1/query", nil)
	q := req.URL.Query()
	q.Add("query", "up{job=\"prometheus\"}")
	q.Add("time", "1234567890")
	req.URL.RawQuery = q.Encode()

	result := extractor.Extract(req)
	require.NotEmpty(t, result)

	var data map[string]interface{}
	require.NoError(t, json.Unmarshal(result, &data))

	assert.Equal(t, "1234567890", data["time"])
	assert.Equal(t, "up{job=\"prometheus\"}", data["query"])
}

func TestRangedQueryExtractor(t *testing.T) {
	extractor := &RangedQueryExtractor{}
	req := httptest.NewRequest("GET", "/api/v1/query_range", nil)
	q := req.URL.Query()
	q.Add("query", "rate(http_requests_total[5m])")
	q.Add("start", "1000")
	q.Add("end", "2000")
	q.Add("step", "15")
	req.URL.RawQuery = q.Encode()

	result := extractor.Extract(req)
	require.NotEmpty(t, result)

	var data map[string]interface{}
	require.NoError(t, json.Unmarshal(result, &data))

	assert.Equal(t, "1000", data["start"])
	assert.Equal(t, "2000", data["end"])
	assert.Equal(t, "15", data["step"])
	assert.Equal(t, "rate(http_requests_total[5m])", data["query"])
}

func TestLongQueryTruncate(t *testing.T) {
	longQuery := strings.Repeat("metric_name{label=\"value\"} or ", maxEntrySize*2) + "final_metric"
	req := httptest.NewRequest("GET", "/api/v1/query", nil)
	q := req.URL.Query()
	q.Add("query", longQuery)
	q.Add("time", "1234567890")
	req.URL.RawQuery = q.Encode()

	extractor := &InstantQueryExtractor{}
	extractedData := extractor.Extract(req)

	require.NotEmpty(t, extractedData)
	assert.True(t, len(extractedData) > 0)
	assert.LessOrEqual(t, len(extractedData), maxEntrySize)
	assert.Contains(t, string(extractedData), "metric_name")
	assert.Contains(t, string(extractedData), "1234567890")
	assert.NotContains(t, string(extractedData), "final_metric")
}
