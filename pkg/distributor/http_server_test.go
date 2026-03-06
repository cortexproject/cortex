package distributor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/ingester/client"
)

func TestTSDBStatusHandler(t *testing.T) {
	distributors, ingesters, _, _ := prepare(t, prepConfig{
		numIngesters:      3,
		happyIngesters:    3,
		numDistributors:   1,
		replicationFactor: 3,
	})

	for _, ing := range ingesters {
		ing.tsdbStatus = client.TSDBStatusResponse{
			NumSeries:    300,
			MinTime:      1000,
			MaxTime:      2000,
			NumLabelPairs: 5,
			SeriesCountByMetricName: []*client.TSDBStatItem{
				{Name: "http_requests_total", Value: 150},
			},
		}
	}

	tests := []struct {
		name        string
		queryString string
	}{
		{"default limit", ""},
		{"custom limit", "?limit=5"},
		{"invalid limit uses default", "?limit=abc"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), "test")
			req := httptest.NewRequest("GET", "/api/v1/status/tsdb"+tc.queryString, nil)
			req = req.WithContext(ctx)
			rec := httptest.NewRecorder()

			distributors[0].TSDBStatusHandler(rec, req)

			assert.Equal(t, http.StatusOK, rec.Code)

			var result TSDBStatusResult
			err := json.Unmarshal(rec.Body.Bytes(), &result)
			require.NoError(t, err)
			assert.Equal(t, uint64(300), result.NumSeries)
			assert.Equal(t, int64(1000), result.MinTime)
			assert.Equal(t, int64(2000), result.MaxTime)
			require.Len(t, result.SeriesCountByMetricName, 1)
			assert.Equal(t, "http_requests_total", result.SeriesCountByMetricName[0].Name)
		})
	}
}
