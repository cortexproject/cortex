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

			if startTime, err := util.ParseTime(startParam); err == nil {
				if endTime, err := util.ParseTime(endParam); err == nil {
					if isWithinTimeAttributes(attribute, now, startTime, endTime) {
						return priority.Priority
					}
				}
			}

			if instantTime, err := util.ParseTime(timeParam); err == nil {
				if isWithinTimeAttributes(attribute, now, instantTime, instantTime) {
					return priority.Priority
				}
			}

			if timeParam == "" {
				if isWithinTimeAttributes(attribute, now, util.TimeToMillis(now), util.TimeToMillis(now)) {
					return priority.Priority
				}
			}
		}
	}

	return queryPriority.DefaultPriority
}

func isWithinTimeAttributes(attribute validation.QueryAttribute, now time.Time, startTime, endTime int64) bool {
	if attribute.StartTime == 0 && attribute.EndTime == 0 {
		return true
	}

	if attribute.StartTime != 0 {
		startTimeThreshold := now.Add(-1 * time.Duration(attribute.StartTime).Abs()).Truncate(time.Second)

		if util.TimeFromMillis(startTime).Before(startTimeThreshold) {
			return false
		}
	}

	if attribute.EndTime != 0 {
		endTimeThreshold := now.Add(-1 * time.Duration(attribute.EndTime).Abs()).Add(1 * time.Second).Truncate(time.Second)

		if util.TimeFromMillis(endTime).After(endTimeThreshold) {
			return false
		}
	}

	return true
}
