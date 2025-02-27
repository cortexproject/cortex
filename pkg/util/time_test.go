package util

import (
	"bytes"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/util/test"
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
		t.Run(fmt.Sprint(i), func(t *testing.T) {
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

func TestDurationWithPositiveJitter(t *testing.T) {
	const numRuns = 1000

	for i := 0; i < numRuns; i++ {
		actual := DurationWithPositiveJitter(time.Minute, 0.5)
		assert.GreaterOrEqual(t, int64(actual), int64(60*time.Second))
		assert.LessOrEqual(t, int64(actual), int64(90*time.Second))
	}
}

func TestDurationWithPositiveJitter_ZeroInputDuration(t *testing.T) {
	assert.Equal(t, time.Duration(0), DurationWithPositiveJitter(time.Duration(0), 0.5))
}

func TestParseTime(t *testing.T) {
	var tests = []struct {
		input  string
		fail   bool
		result time.Time
	}{
		{
			input: "",
			fail:  true,
		}, {
			input: "abc",
			fail:  true,
		}, {
			input: "30s",
			fail:  true,
		}, {
			input:  "123",
			result: time.Unix(123, 0),
		}, {
			input:  "123.123",
			result: time.Unix(123, 123000000),
		}, {
			input:  "2015-06-03T13:21:58.555Z",
			result: time.Unix(1433337718, 555*time.Millisecond.Nanoseconds()),
		}, {
			input:  "2015-06-03T14:21:58.555+01:00",
			result: time.Unix(1433337718, 555*time.Millisecond.Nanoseconds()),
		}, {
			// Test nanosecond rounding.
			input:  "2015-06-03T13:21:58.56789Z",
			result: time.Unix(1433337718, 567*1e6),
		}, {
			// Test float rounding.
			input:  "1543578564.705",
			result: time.Unix(1543578564, 705*1e6),
		},
	}

	for _, test := range tests {
		ts, err := ParseTime(test.input)
		if test.fail {
			require.Error(t, err)
			continue
		}

		require.NoError(t, err)
		assert.Equal(t, TimeToMillis(test.result), ts)
	}
}

func TestNewDisableableTicker_Enabled(t *testing.T) {
	stop, ch := NewDisableableTicker(10 * time.Millisecond)
	defer stop()

	time.Sleep(100 * time.Millisecond)

	select {
	case <-ch:
		break
	default:
		t.Error("ticker should have ticked when enabled")
	}
}

func TestNewDisableableTicker_Disabled(t *testing.T) {
	stop, ch := NewDisableableTicker(0)
	defer stop()

	time.Sleep(100 * time.Millisecond)

	select {
	case <-ch:
		t.Error("ticker should not have ticked when disabled")
	default:
		break
	}
}

func TestFindMinMaxTime(t *testing.T) {
	now := time.Now()

	type testCase struct {
		query           string
		lookbackDelta   time.Duration
		queryStartTime  time.Time
		queryEndTime    time.Time
		expectedMinTime time.Time
		expectedMaxTime time.Time
	}

	tests := map[string]testCase{
		"should consider min and max of the query param": {
			query:           "up",
			queryStartTime:  now.Add(-1 * time.Hour),
			queryEndTime:    now,
			expectedMinTime: now.Add(-1 * time.Hour),
			expectedMaxTime: now,
		},
		"should consider min and max of inner queries": {
			query:           "go_gc_duration_seconds_count[2h] offset 30m + go_gc_duration_seconds_count[3h] offset 1h",
			queryStartTime:  now.Add(-1 * time.Hour),
			queryEndTime:    now,
			expectedMinTime: now.Add(-5 * time.Hour),
			expectedMaxTime: now.Add(-30 * time.Minute),
		},
		"should consider lookback delta": {
			query:           "up",
			lookbackDelta:   1 * time.Hour,
			queryStartTime:  now.Add(-1 * time.Hour),
			queryEndTime:    now,
			expectedMinTime: now.Add(-2 * time.Hour),
			expectedMaxTime: now,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			expr, _ := parser.ParseExpr(testData.query)

			url := "/query_range?query=" + testData.query +
				"&start=" + strconv.FormatInt(testData.queryStartTime.Truncate(time.Minute).Unix(), 10) +
				"&end=" + strconv.FormatInt(testData.queryEndTime.Truncate(time.Minute).Unix(), 10)
			req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader([]byte{}))

			minTime, maxTime := FindMinMaxTime(req, expr, testData.lookbackDelta, now)
			assert.Equal(t, testData.expectedMinTime.Truncate(time.Minute).UnixMilli()+1, minTime) // refer to https://github.com/prometheus/prometheus/issues/13213 for the reason +1
			assert.Equal(t, testData.expectedMaxTime.Truncate(time.Minute).UnixMilli(), maxTime)
		})
	}
}

func TestSlottedTicker(t *testing.T) {
	t.Parallel()
	testCases := map[string]struct {
		duration   time.Duration
		totalSlots int
		slotNumber int
	}{
		"No Slots should spread across all the interval": {
			duration:   300 * time.Millisecond,
			totalSlots: 1,
			slotNumber: 0,
		},
		"Get first slot": {
			duration:   300 * time.Millisecond,
			totalSlots: 5,
			slotNumber: 0,
		},
		"Get 3th slot": {
			duration:   300 * time.Millisecond,
			totalSlots: 5,
			slotNumber: 3,
		},
		"Get last slot": {
			duration:   300 * time.Millisecond,
			totalSlots: 5,
			slotNumber: 4,
		},
	}
	for name, c := range testCases {
		tc := c
		t.Run(name, func(t *testing.T) {
			infoFunc := func() (int, int) {
				return tc.slotNumber, tc.totalSlots
			}
			ticker := NewSlottedTicker(infoFunc, tc.duration, 0)
			slotSize := tc.duration.Milliseconds() / int64(tc.totalSlots)
			successCount := 0

			test.Poll(t, 5*time.Second, true, func() interface{} {
				tTime := <-ticker.C
				slotShiftInMs := tTime.UnixMilli() % tc.duration.Milliseconds()
				slot := slotShiftInMs / slotSize
				if slot == int64(tc.slotNumber) {
					successCount++
				} else {
					successCount--
				}

				return successCount == 10
			})
			ticker.Stop()
		})
	}

	t.Run("Change slot size", func(t *testing.T) {
		slotSize := atomic.NewInt32(10)
		d := 300 * time.Millisecond
		infoFunc := func() (int, int) {
			return 2, int(slotSize.Load())
		}

		ticker := NewSlottedTicker(infoFunc, d, 0)

		test.Poll(t, 5*time.Second, true, func() interface{} {
			tTime := <-ticker.C
			slotShiftInMs := tTime.UnixMilli() % d.Milliseconds()
			return slotShiftInMs >= 60 && slotShiftInMs <= 90
		})
		slotSize.Store(5)
		test.Poll(t, 2*time.Second, true, func() interface{} {
			tTime := <-ticker.C
			slotShiftInMs := tTime.UnixMilli() % d.Milliseconds()
			return slotShiftInMs >= 120 && slotShiftInMs <= 180
		})
	})
}
