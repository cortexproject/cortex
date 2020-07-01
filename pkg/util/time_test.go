package util

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimeFromMillis(t *testing.T) {
	var testExpr = []struct {
		input    int64
		expected time.Time
	}{
		{input: 1000, expected: time.Unix(1, 0)},
		{input: 1500, expected: time.Unix(1, 500*nanosecondsInMillisecond)},
	}

	for i, c := range testExpr {
		t.Run(string(i), func(t *testing.T) {
			res := TimeFromMillis(c.input)
			require.Equal(t, c.expected, res)
		})
	}
}

func TestDurationWithJitter(t *testing.T) {
	const numRuns = 1000

	for i := 0; i < numRuns; i++ {
		actual := DurationWithJitter(time.Minute, 0.5)
		assert.GreaterOrEqual(t, int64(actual), int64(30*time.Second))
		assert.LessOrEqual(t, int64(actual), int64(90*time.Second))
	}
}

func TestDurationWithJitter_ZeroInputDuration(t *testing.T) {
	assert.Equal(t, time.Duration(0), DurationWithJitter(time.Duration(0), 0.5))
}
