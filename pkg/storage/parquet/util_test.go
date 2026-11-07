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
		checkFunc  func() bool
	}{
		{
			name:      "2h block. Don't convert",
			mint:      0,
			maxt:      2 * time.Hour.Milliseconds(),
			durations: tsdb.DurationList{2 * time.Hour, 12 * time.Hour},
			expected:  false,
			checkFunc: func() bool {
				return false
			},
		},
		{
			name:      "1h block. Don't convert",
			mint:      0,
			maxt:      1 * time.Hour.Milliseconds(),
			durations: tsdb.DurationList{2 * time.Hour, 12 * time.Hour},
			expected:  false,
			checkFunc: func() bool {
				return false
			},
		},
		{
			name:      "2h block. Exist NoCompactMark. Convert",
			mint:      0,
			maxt:      2 * time.Hour.Milliseconds(),
			durations: tsdb.DurationList{2 * time.Hour, 12 * time.Hour},
			expected:  true,
			checkFunc: func() bool {
				return true
			},
		},
		{
			name:      "1h block. Exist NoCompactMark. Convert",
			mint:      0,
			maxt:      1 * time.Hour.Milliseconds(),
			durations: tsdb.DurationList{2 * time.Hour, 12 * time.Hour},
			expected:  true,
			checkFunc: func() bool {
				return true
			},
		},
		{
			name:      "3h block. Convert",
			mint:      0,
			maxt:      3 * time.Hour.Milliseconds(),
			durations: tsdb.DurationList{2 * time.Hour, 12 * time.Hour},
			expected:  true,
			checkFunc: func() bool {
				return false
			},
		},
		{
			name:      "12h block. Convert",
			mint:      0,
			maxt:      12 * time.Hour.Milliseconds(),
			durations: tsdb.DurationList{2 * time.Hour, 12 * time.Hour},
			expected:  true,
			checkFunc: func() bool {
				return false
			},
		},
		{
			name:      "12h block with 1h offset. Convert",
			mint:      time.Hour.Milliseconds(),
			maxt:      13 * time.Hour.Milliseconds(),
			durations: tsdb.DurationList{2 * time.Hour, 12 * time.Hour},
			expected:  true,
			checkFunc: func() bool {
				return false
			},
		},
		{
			name:      "24h block. Convert",
			mint:      0,
			maxt:      24 * time.Hour.Milliseconds(),
			durations: tsdb.DurationList{2 * time.Hour, 12 * time.Hour},
			expected:  true,
			checkFunc: func() bool {
				return false
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := tc.maxt + (24 * time.Hour).Milliseconds()
			res := ShouldConvertBlockToParquet(tc.mint, tc.maxt, now, (&tc.durations).ToMilliseconds(), tc.checkFunc)
			require.Equal(t, tc.expected, res)
		})
	}
}

func TestShouldConvertBlockToParquet_NoCompactMarkCheckAge(t *testing.T) {
	const checkAfter = 12 * time.Hour

	// Real timestamps are used on purpose: the block's age has to be computed
	// from the current time, not by comparing a timestamp with a duration.
	maxt := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	mint := maxt.Add(-2 * time.Hour)

	for _, tc := range []struct {
		name          string
		durations     tsdb.DurationList
		age           time.Duration
		hasMark       bool
		expectedCheck bool
		expected      bool
	}{
		{
			name:    "block ended one hour ago. Don't check the marker",
			age:     time.Hour,
			hasMark: true,
		},
		{
			name:    "block is just younger than the threshold. Don't check the marker",
			age:     checkAfter - time.Millisecond,
			hasMark: true,
		},
		{
			name:          "block is exactly as old as the threshold. Convert",
			age:           checkAfter,
			hasMark:       true,
			expectedCheck: true,
			expected:      true,
		},
		{
			name:          "block is older than the threshold. Convert",
			age:           48 * time.Hour,
			hasMark:       true,
			expectedCheck: true,
			expected:      true,
		},
		{
			name:          "block is older than the threshold without marker. Don't convert",
			age:           48 * time.Hour,
			expectedCheck: true,
		},
		{
			name:    "block ends in the future. Don't check the marker",
			age:     -time.Hour,
			hasMark: true,
		},
		{
			name:      "threshold follows the configured block ranges. Don't check the marker",
			durations: tsdb.DurationList{2 * time.Hour, 24 * time.Hour},
			age:       checkAfter,
			hasMark:   true,
		},
		{
			name:          "single block range uses that range as threshold. Convert",
			durations:     tsdb.DurationList{2 * time.Hour},
			age:           2 * time.Hour,
			hasMark:       true,
			expectedCheck: true,
			expected:      true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checked := false
			checkFunc := func() bool {
				checked = true
				return tc.hasMark
			}

			durations := tc.durations
			if durations == nil {
				durations = tsdb.DurationList{2 * time.Hour, checkAfter}
			}

			res := ShouldConvertBlockToParquet(mint.UnixMilli(), maxt.UnixMilli(), maxt.Add(tc.age).UnixMilli(), (&durations).ToMilliseconds(), checkFunc)
			require.Equal(t, tc.expected, res)
			require.Equal(t, tc.expectedCheck, checked)
		})
	}
}
