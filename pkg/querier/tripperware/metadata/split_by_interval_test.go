package metadata

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/querier/tripperware/queryrange"
)

func TestSplitSeries(t *testing.T) {

	for _, tc := range []struct {
		name           string
		request        tripperware.Request
		expectedSplits []tripperware.Request
		interval       time.Duration
		expectedErr    error
	}{
		{
			name: "Single day",
			request: &PrometheusSeriesRequest{
				Path:  "/api/v1/series",
				Start: 1640995200 * 1e3,
				End:   (1640995200 + 23*3600) * 1e3,
			},
			interval: 24 * time.Hour,
			expectedSplits: []tripperware.Request{
				&PrometheusSeriesRequest{
					Path:  "/api/v1/series",
					Start: 1640995200 * 1e3,
					End:   (1640995200 + 23*3600) * 1e3,
				},
			},
		},
		{
			name: "Two days",
			request: &PrometheusSeriesRequest{
				Path:  "/api/v1/series",
				Start: 1640995200 * 1e3,
				End:   (1640995200 + 24*3600 + 23*3600) * 1e3,
			},
			interval: 24 * time.Hour,
			expectedSplits: []tripperware.Request{
				&PrometheusSeriesRequest{
					Path:  "/api/v1/series",
					Start: 1640995200 * 1e3,
					End:   (1640995200 + 24*3600) * 1e3,
				},
				&PrometheusSeriesRequest{
					Path:  "/api/v1/series",
					Start: (1640995200 + 24*3600) * 1e3,
					End:   (1640995200 + 24*3600 + 23*3600) * 1e3,
				},
			},
		},
		{
			name: "Imperfect",
			request: &PrometheusSeriesRequest{
				Path:  "/api/v1/series",
				Start: 1640995200021,
				End:   1640995200021 + (24*3600+23*3600)*1e3,
			},
			interval: 24 * time.Hour,
			expectedSplits: []tripperware.Request{
				&PrometheusSeriesRequest{
					Path:  "/api/v1/series",
					Start: 1640995200021,
					End:   (1640995200 + 24*3600) * 1e3,
				},
				&PrometheusSeriesRequest{
					Path:  "/api/v1/series",
					Start: (1640995200 + 24*3600) * 1e3,
					End:   1640995200021 + (24*3600+23*3600)*1e3,
				},
			},
		},
		{
			name: "Imperfect 1h interval",
			request: &PrometheusSeriesRequest{
				Path:  "/api/v1/series",
				Start: 1641024001 * 1e3,
				End:   (1641024001 + 3*3600 + 1800) * 1e3,
			},
			interval: 1 * time.Hour,
			expectedSplits: []tripperware.Request{
				&PrometheusSeriesRequest{
					Path:  "/api/v1/series",
					Start: 1641024001 * 1e3,
					End:   (1641024000 + 3600) * 1e3,
				},
				&PrometheusSeriesRequest{
					Path:  "/api/v1/series",
					Start: (1641024000 + 3600) * 1e3,
					End:   (1641024000 + 2*3600) * 1e3,
				},
				&PrometheusSeriesRequest{
					Path:  "/api/v1/series",
					Start: (1641024000 + 2*3600) * 1e3,
					End:   (1641024000 + 3*3600) * 1e3,
				},
				&PrometheusSeriesRequest{
					Path:  "/api/v1/series",
					Start: (1641024000 + 3*3600) * 1e3,
					End:   (1641024001 + 3*3600 + 1800) * 1e3,
				},
			},
		},
		{
			name: "Start > End",
			request: &PrometheusSeriesRequest{
				Path:  "/api/v1/series",
				Start: 1666721823 * 1e3,
				End:   1666721731 * 1e3,
			},
			interval:       1 * time.Hour,
			expectedSplits: []tripperware.Request{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			splits, err := queryrange.SplitQuery(tc.request, tc.interval)
			require.NoError(t, err)

			require.ElementsMatch(t, tc.expectedSplits, splits)
		})
	}
}
