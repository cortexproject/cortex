package query

import (
	"math"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"github.com/pkg/errors"

	"github.com/cortexproject/cortex/pkg/util/validation"
)

func GetPriority(requestParams url.Values, now time.Time, queryPriority *validation.QueryPriority, queryPriorityChanged bool) int64 {
	queryParam := requestParams.Get("query")
	timeParam := requestParams.Get("time")
	startParam := requestParams.Get("start")
	endParam := requestParams.Get("end")

	if queryParam == "" || !queryPriority.Enabled {
		return queryPriority.DefaultPriority
	}

	for i, priority := range queryPriority.Priorities {
		for j, attribute := range priority.QueryAttributes {
			if queryPriorityChanged {
				compiledRegex, err := regexp.Compile(attribute.Regex)
				if err != nil {
					continue
				}

				queryPriority.Priorities[i].QueryAttributes[j].CompiledRegex = compiledRegex
			}

			if attribute.CompiledRegex != nil && !attribute.CompiledRegex.MatchString(queryParam) {
				continue
			}

			startTimeThreshold := now.Add(-1 * attribute.StartTime.Abs()).Truncate(time.Second).UTC()
			endTimeThreshold := now.Add(-1 * attribute.EndTime.Abs()).Round(time.Second).UTC()

			if instantTime, err := parseTime(timeParam); err == nil {
				if isBetweenThresholds(instantTime, instantTime, startTimeThreshold, endTimeThreshold) {
					return priority.Priority
				}
			}

			if startTime, err := parseTime(startParam); err == nil {
				if endTime, err := parseTime(endParam); err == nil {
					if isBetweenThresholds(startTime, endTime, startTimeThreshold, endTimeThreshold) {
						return priority.Priority
					}
				}
			}
		}
	}

	return queryPriority.DefaultPriority
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
	return (start.Equal(startThreshold) || start.After(startThreshold)) && (end.Equal(endThreshold) || end.Before(endThreshold))
}
