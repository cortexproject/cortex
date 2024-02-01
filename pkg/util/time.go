package util

import (
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/weaveworks/common/httpgrpc"
)

const (
	nanosecondsInMillisecond = int64(time.Millisecond / time.Nanosecond)
)

func TimeToMillis(t time.Time) int64 {
	return t.UnixNano() / nanosecondsInMillisecond
}

// TimeFromMillis is a helper to turn milliseconds -> time.Time
func TimeFromMillis(ms int64) time.Time {
	return time.Unix(0, ms*nanosecondsInMillisecond)
}

// FormatTimeMillis returns a human readable version of the input time (in milliseconds).
func FormatTimeMillis(ms int64) string {
	return TimeFromMillis(ms).String()
}

// FormatTimeModel returns a human readable version of the input time.
func FormatTimeModel(t model.Time) string {
	return TimeFromMillis(int64(t)).String()
}

func FormatMillisToSeconds(ms int64) string {
	return strconv.FormatFloat(float64(ms)/float64(1000), 'f', -1, 64)
}

// ParseTime parses the string into an int64, milliseconds since epoch.
func ParseTime(s string) (int64, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000
		tm := time.Unix(int64(s), int64(ns*float64(time.Second)))
		return TimeToMillis(tm), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return TimeToMillis(t), nil
	}
	return 0, httpgrpc.Errorf(http.StatusBadRequest, "cannot parse %q to a valid timestamp", s)
}

// ParseTimeParam parses the time request parameter into an int64, milliseconds since epoch.
func ParseTimeParam(r *http.Request, paramName string, defaultValue int64) (int64, error) {
	val := r.FormValue(paramName)
	if val == "" {
		val = strconv.FormatInt(defaultValue, 10)
	}
	result, err := ParseTime(val)
	if err != nil {
		return 0, errors.Wrapf(err, "Invalid time value for '%s'", paramName)
	}
	return result, nil
}

// DurationWithJitter returns random duration from "input - input*variance" to "input + input*variance" interval.
func DurationWithJitter(input time.Duration, variancePerc float64) time.Duration {
	// No duration? No jitter.
	if input == 0 {
		return 0
	}

	variance := int64(float64(input) * variancePerc)
	jitter := rand.Int63n(variance*2) - variance

	return input + time.Duration(jitter)
}

// DurationWithPositiveJitter returns random duration from "input" to "input + input*variance" interval.
func DurationWithPositiveJitter(input time.Duration, variancePerc float64) time.Duration {
	// No duration? No jitter.
	if input == 0 {
		return 0
	}

	variance := int64(float64(input) * variancePerc)
	jitter := rand.Int63n(variance)

	return input + time.Duration(jitter)
}

// NewDisableableTicker essentially wraps NewTicker but allows the ticker to be disabled by passing
// zero duration as the interval. Returns a function for stopping the ticker, and the ticker channel.
func NewDisableableTicker(interval time.Duration) (func(), <-chan time.Time) {
	if interval == 0 {
		return func() {}, nil
	}

	tick := time.NewTicker(interval)
	return func() { tick.Stop() }, tick.C
}

// FindMinMaxTime returns the time in milliseconds of the earliest and latest point in time the statement will try to process.
// This takes into account offsets, @ modifiers, and range selectors.
// If the expression does not select series, then FindMinMaxTime returns (0, 0).
func FindMinMaxTime(r *http.Request, expr parser.Expr, lookbackDelta time.Duration, now time.Time) (int64, int64) {
	isQuery := strings.HasSuffix(r.URL.Path, "/query")

	var startTime, endTime int64
	if isQuery {
		if t, err := ParseTimeParam(r, "time", now.UnixMilli()); err == nil {
			startTime = t
			endTime = t
		}
	} else {
		if st, err := ParseTime(r.FormValue("start")); err == nil {
			if et, err := ParseTime(r.FormValue("end")); err == nil {
				startTime = st
				endTime = et
			}
		}
	}

	es := &parser.EvalStmt{
		Expr:          expr,
		Start:         TimeFromMillis(startTime),
		End:           TimeFromMillis(endTime),
		LookbackDelta: lookbackDelta,
	}

	return promql.FindMinMaxTime(es)
}
