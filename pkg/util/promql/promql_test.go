package promql

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

func TestFindNonOverlapQueryLength(t *testing.T) {
	for _, tc := range []struct {
		name           string
		query          string
		expectedLength time.Duration
	}{
		{
			name:  "number literal, no select",
			query: `1`,
		},
		{
			name:  "string literal, no select",
			query: `"test"`,
		},
		{
			name:  "function, no select",
			query: `"time()"`,
		},
		{
			name:           "single vector selector",
			query:          `up`,
			expectedLength: time.Minute * 5,
		},
		{
			name:           "single matrix selector",
			query:          `up[1h]`,
			expectedLength: time.Hour,
		},
		{
			name:           "sum rate",
			query:          `sum(rate(up[1h]))`,
			expectedLength: time.Hour,
		},
		{
			name:           "single vector selector with offset",
			query:          `up offset 7d`,
			expectedLength: time.Minute * 5,
		},
		{
			name:           "single matrix selector with offset",
			query:          `up[1h] offset 7d`,
			expectedLength: time.Hour,
		},
		{
			name:           "multiple vector selectors, dedup time range",
			query:          `sum(up) + sum(up) + sum(up)`,
			expectedLength: time.Minute * 5,
		},
		{
			name:           "multiple matrix selectors, dedup time range",
			query:          `sum_over_time(up[1h]) + sum_over_time(up[1h]) + sum_over_time(up[1h])`,
			expectedLength: time.Hour,
		},
		{
			name:           "multiple vector selectors with offsets",
			query:          `sum(up) + sum(up offset 1h) + sum(up offset 2h)`,
			expectedLength: time.Minute * 15,
		},
		{
			name:           "multiple matrix selectors with offsets",
			query:          `sum_over_time(up[1h]) + sum_over_time(up[1h] offset 1d) + sum_over_time(up[1h] offset 2d)`,
			expectedLength: time.Hour * 3,
		},
		{
			name:           "multiple sum rate with offsets",
			query:          `sum(rate(up[5m])) + sum(rate(up[5m] offset 1w)) + sum(rate(up[5m] offset 2w)) + sum(rate(up[5m] offset 3w)) + sum(rate(up[5m] offset 4w))`,
			expectedLength: time.Minute * 5 * 5,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := parser.ParseExpr(tc.query)
			require.NoError(t, err)
			duration := FindNonOverlapQueryLength(expr, 0, 0, time.Minute*5)
			require.Equal(t, tc.expectedLength, duration)
		})
	}
}
