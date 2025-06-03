package api

import (
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/util"
)

var (
	// MinTime is the default timestamp used for the start of optional time ranges.
	// Exposed to let downstream projects reference it.
	//
	// Historical note: This should just be time.Unix(math.MinInt64/1000, 0).UTC(),
	// but it was set to a higher value in the past due to a misunderstanding.
	// The value is still low enough for practical purposes, so we don't want
	// to change it now, avoiding confusion for importers of this variable.
	MinTime = time.Unix(math.MinInt64/1000+62135596801, 0).UTC()

	// MaxTime is the default timestamp used for the end of optional time ranges.
	// Exposed to let downstream projects to reference it.
	//
	// Historical note: This should just be time.Unix(math.MaxInt64/1000, 0).UTC(),
	// but it was set to a lower value in the past due to a misunderstanding.
	// The value is still high enough for practical purposes, so we don't want
	// to change it now, avoiding confusion for importers of this variable.
	MaxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC()

	minTimeFormatted = MinTime.Format(time.RFC3339Nano)
	maxTimeFormatted = MaxTime.Format(time.RFC3339Nano)
)

func ParseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000
		return time.Unix(int64(s), int64(ns*float64(time.Second))).UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}

	// Stdlib's time parser can only handle 4 digit years. As a workaround until
	// that is fixed we want to at least support our own boundary times.
	// Context: https://github.com/prometheus/client_golang/issues/614
	// Upstream issue: https://github.com/golang/go/issues/20555
	switch s {
	case minTimeFormatted:
		return MinTime, nil
	case maxTimeFormatted:
		return MaxTime, nil
	}
	return time.Time{}, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}

func ParseTimeMillis(s string) (int64, error) {
	t, err := ParseTime(s)
	if err != nil {
		return 0, httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error())
	}

	return util.TimeToMillis(t), nil
}

func ParseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
}

func ParseDurationMillis(s string) (int64, error) {
	d, err := ParseDuration(s)
	if err != nil {
		return 0, httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error())
	}

	return int64(d / (time.Millisecond / time.Nanosecond)), nil
}

func ParseTimeParam(r *http.Request, paramName string, defaultValue time.Time) (time.Time, error) {
	val := r.FormValue(paramName)
	if val == "" {
		val = strconv.FormatInt(defaultValue.Unix(), 10)
	}
	result, err := ParseTime(val)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid time value for '%s': %w", paramName, err)
	}
	return result, nil
}

func ParseTimeParamMillis(r *http.Request, paramName string, defaultValue time.Time) (int64, error) {
	t, err := ParseTimeParam(r, paramName, defaultValue)
	if err != nil {
		return 0, httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error())
	}

	return util.TimeToMillis(t), nil
}

// FindMinMaxTime returns the time in milliseconds of the earliest and latest point in time the statement will try to process.
// This takes into account offsets, @ modifiers, and range selectors.
// If the expression does not select series, then FindMinMaxTime returns (0, 0).
func FindMinMaxTime(r *http.Request, expr parser.Expr, lookbackDelta time.Duration, now time.Time) (int64, int64) {
	isQuery := strings.HasSuffix(r.URL.Path, "/query")

	var startTime, endTime int64
	if isQuery {
		if t, err := ParseTimeParamMillis(r, "time", now); err == nil {
			startTime = t
			endTime = t
		}
	} else {
		if st, err := ParseTimeMillis(r.FormValue("start")); err == nil {
			if et, err := ParseTimeMillis(r.FormValue("end")); err == nil {
				startTime = st
				endTime = et
			}
		}
	}

	es := &parser.EvalStmt{
		Expr:          expr,
		Start:         util.TimeFromMillis(startTime),
		End:           util.TimeFromMillis(endTime),
		LookbackDelta: lookbackDelta,
	}

	return promql.FindMinMaxTime(es)
}
