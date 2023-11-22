package query

import (
	"net/url"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func GetPriority(requestParams url.Values, now time.Time, queryPriority validation.QueryPriority) int64 {
	queryParam := requestParams.Get("query")
	timeParam := requestParams.Get("time")
	startParam := requestParams.Get("start")
	endParam := requestParams.Get("end")

	if queryParam == "" || !queryPriority.Enabled {
		return queryPriority.DefaultPriority
	}

	for _, priority := range queryPriority.Priorities {
		for _, attribute := range priority.QueryAttributes {
			if attribute.CompiledRegex != nil && !attribute.CompiledRegex.MatchString(queryParam) {
				continue
			}

			startTimeThreshold := now.Add(-1 * attribute.StartTime.Abs()).Truncate(time.Second).UTC()
			endTimeThreshold := now.Add(-1 * attribute.EndTime.Abs()).Add(1 * time.Second).Truncate(time.Second).UTC()

			if startTime, err := util.ParseTime(startParam); err == nil {
				if endTime, err := util.ParseTime(endParam); err == nil {
					if isBetweenThresholds(util.TimeFromMillis(startTime), util.TimeFromMillis(endTime), startTimeThreshold, endTimeThreshold) {
						return priority.Priority
					}
				}
			}

			if instantTime, err := util.ParseTime(timeParam); err == nil {
				if isBetweenThresholds(util.TimeFromMillis(instantTime), util.TimeFromMillis(instantTime), startTimeThreshold, endTimeThreshold) {
					return priority.Priority
				}
			}

			if timeParam == "" {
				if isBetweenThresholds(now, now, startTimeThreshold, endTimeThreshold) {
					return priority.Priority
				}
			}
		}
	}

	return queryPriority.DefaultPriority
}

func isBetweenThresholds(start, end, startThreshold, endThreshold time.Time) bool {
	return (start.Equal(startThreshold) || start.After(startThreshold)) && (end.Equal(endThreshold) || end.Before(endThreshold))
}
