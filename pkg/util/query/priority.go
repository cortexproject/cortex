package query

import (
	"math"
	"net/url"
	"strconv"
	"time"

	"github.com/pkg/errors"

	"github.com/cortexproject/cortex/pkg/util/validation"
)

func IsHighPriority(requestParams url.Values, now time.Time, highPriorityQueries []validation.HighPriorityQuery) bool {
	queryParam := requestParams.Get("query")
	timeParam := requestParams.Get("time")
	startParam := requestParams.Get("start")
	endParam := requestParams.Get("end")

	if queryParam == "" {
		return false
	}

	for _, highPriorityQuery := range highPriorityQueries {
		compiledRegex := highPriorityQuery.CompiledRegex

		if compiledRegex == nil || !compiledRegex.MatchString(queryParam) {
			continue
		}

		startTimeThreshold := now.Add(-1 * highPriorityQuery.StartTime.Abs())
		endTimeThreshold := now.Add(-1 * highPriorityQuery.EndTime.Abs())

		if instantTime, err := parseTime(timeParam); err == nil {
			if isBetweenThresholds(instantTime, instantTime, startTimeThreshold, endTimeThreshold) {
				return true
			}
		}

		if startTime, err := parseTime(startParam); err == nil {
			if endTime, err := parseTime(endParam); err == nil {
				if isBetweenThresholds(startTime, endTime, startTimeThreshold, endTimeThreshold) {
					return true
				}
			}
		}
	}

	return false
}

func parseTime(s string) (time.Time, error) {
	if s != "" {
		if t, err := strconv.ParseFloat(s, 64); err == nil {
			s, ns := math.Modf(t)
			ns = math.Round(ns*1000) / 1000
			return time.Unix(int64(s), int64(ns*float64(time.Second))).UTC(), nil
		}
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return t, nil
		}
	}

	return time.Time{}, errors.Errorf("cannot parse %q to a valid timestamp", s)
}

func isBetweenThresholds(start, end, startThreshold, endThreshold time.Time) bool {
	return start.After(startThreshold) && end.Before(endThreshold)
}
