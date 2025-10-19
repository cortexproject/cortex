package parquet

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/storage/tsdb"
)

func TestShouldConvertBlockToParquet(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mint, maxt int64
		durations  tsdb.DurationList
		expected   bool
	}{
		{
			name:      "2h block. Don't convert",
			mint:      0,
			maxt:      2 * time.Hour.Milliseconds(),
			durations: tsdb.DurationList{2 * time.Hour, 12 * time.Hour},
			expected:  false,
		},
		{
			name:      "1h block. Don't convert",
			mint:      0,
			maxt:      1 * time.Hour.Milliseconds(),
			durations: tsdb.DurationList{2 * time.Hour, 12 * time.Hour},
			expected:  false,
		},
		{
			name:      "3h block. Convert",
			mint:      0,
			maxt:      3 * time.Hour.Milliseconds(),
			durations: tsdb.DurationList{2 * time.Hour, 12 * time.Hour},
			expected:  true,
		},
		{
			name:      "12h block. Convert",
			mint:      0,
			maxt:      12 * time.Hour.Milliseconds(),
			durations: tsdb.DurationList{2 * time.Hour, 12 * time.Hour},
			expected:  true,
		},
		{
			name:      "12h block with 1h offset. Convert",
			mint:      time.Hour.Milliseconds(),
			maxt:      13 * time.Hour.Milliseconds(),
			durations: tsdb.DurationList{2 * time.Hour, 12 * time.Hour},
			expected:  true,
		},
		{
			name:      "24h block. Convert",
			mint:      0,
			maxt:      24 * time.Hour.Milliseconds(),
			durations: tsdb.DurationList{2 * time.Hour, 12 * time.Hour},
			expected:  true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := ShouldConvertBlockToParquet(tc.mint, tc.maxt, (&tc.durations).ToMilliseconds())
			require.Equal(t, tc.expected, res)
		})
	}
}
