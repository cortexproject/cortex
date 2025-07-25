package parquet

import (
	"testing"
	"time"

	"crypto/rand"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/storage/tsdb"
)

func TestShouldConvertBlockToParquet(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		mint, maxt              int64
		noCompactMarkCheckAfter int64
		durations               tsdb.DurationList
		expected                bool
		checkFunc               NoCompactMarkCheckFunc
	}{
		{
			name:      "2h block. Don't convert",
			mint:      0,
			maxt:      2 * time.Hour.Milliseconds(),
			durations: tsdb.DurationList{2 * time.Hour, 12 * time.Hour},
			expected:  false,
			checkFunc: func(bId ulid.ULID) bool {
				return false
			},
		},
		{
			name:      "1h block. Don't convert",
			mint:      0,
			maxt:      1 * time.Hour.Milliseconds(),
			durations: tsdb.DurationList{2 * time.Hour, 12 * time.Hour},
			expected:  false,
			checkFunc: func(bId ulid.ULID) bool {
				return false
			},
		},
		{
			name:      "2h block. Exist NoCompactMark. Convert",
			mint:      0,
			maxt:      2 * time.Hour.Milliseconds(),
			durations: tsdb.DurationList{2 * time.Hour, 12 * time.Hour},
			expected:  true,
			checkFunc: func(bId ulid.ULID) bool {
				return true
			},
		},
		{
			name:                    "2h block. Exist NoCompactMark. noCompactMarkCheckAfter is one hour. Not Convert",
			mint:                    0,
			maxt:                    2 * time.Hour.Milliseconds(),
			noCompactMarkCheckAfter: 1 * time.Hour.Milliseconds(),
			durations:               tsdb.DurationList{2 * time.Hour, 12 * time.Hour},
			expected:                false,
			checkFunc: func(bId ulid.ULID) bool {
				return true
			},
		},
		{
			name:      "1h block. Exist NoCompactMark. Convert",
			mint:      0,
			maxt:      1 * time.Hour.Milliseconds(),
			durations: tsdb.DurationList{2 * time.Hour, 12 * time.Hour},
			expected:  true,
			checkFunc: func(bId ulid.ULID) bool {
				return true
			},
		},
		{
			name:      "3h block. Convert",
			mint:      0,
			maxt:      3 * time.Hour.Milliseconds(),
			durations: tsdb.DurationList{2 * time.Hour, 12 * time.Hour},
			expected:  true,
			checkFunc: func(bId ulid.ULID) bool {
				return false
			},
		},
		{
			name:      "12h block. Convert",
			mint:      0,
			maxt:      12 * time.Hour.Milliseconds(),
			durations: tsdb.DurationList{2 * time.Hour, 12 * time.Hour},
			expected:  true,
			checkFunc: func(bId ulid.ULID) bool {
				return false
			},
		},
		{
			name:      "12h block with 1h offset. Convert",
			mint:      time.Hour.Milliseconds(),
			maxt:      13 * time.Hour.Milliseconds(),
			durations: tsdb.DurationList{2 * time.Hour, 12 * time.Hour},
			expected:  true,
			checkFunc: func(bId ulid.ULID) bool {
				return false
			},
		},
		{
			name:      "24h block. Convert",
			mint:      0,
			maxt:      24 * time.Hour.Milliseconds(),
			durations: tsdb.DurationList{2 * time.Hour, 12 * time.Hour},
			expected:  true,
			checkFunc: func(bId ulid.ULID) bool {
				return false
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id, err := ulid.New(ulid.Now(), rand.Reader)
			require.NoError(t, err)
			res := ShouldConvertBlockToParquet(tc.mint, tc.maxt, tc.noCompactMarkCheckAfter, (&tc.durations).ToMilliseconds(), id, tc.checkFunc)
			require.Equal(t, tc.expected, res)
		})
	}
}
