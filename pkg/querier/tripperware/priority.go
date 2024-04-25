package tripperware

import (
	"time"

	"github.com/cortexproject/cortex/pkg/util/validation"
)

func GetPriority(query string, minTime, maxTime int64, now time.Time, queryPriority validation.QueryPriority) int64 {
	if !queryPriority.Enabled || query == "" || len(queryPriority.Priorities) == 0 {
		return queryPriority.DefaultPriority
	}

	for _, priority := range queryPriority.Priorities {
		for _, attribute := range priority.QueryAttributes {
			if attribute.Regex != "" && attribute.Regex != ".*" && attribute.Regex != ".+" {
				if attribute.CompiledRegex != nil && !attribute.CompiledRegex.MatchString(query) {
					continue
				}
			}

			if isWithinTimeAttributes(attribute.TimeWindow, now, minTime, maxTime) {
				return priority.Priority
			}
		}
	}

	return queryPriority.DefaultPriority
}

func isWithinTimeAttributes(timeWindow validation.TimeWindow, now time.Time, startTime, endTime int64) bool {
	if timeWindow.Start == 0 && timeWindow.End == 0 {
		return true
	}

	if timeWindow.Start != 0 {
		startTimeThreshold := now.Add(-1 * time.Duration(timeWindow.Start).Abs()).Add(-1 * time.Minute).Truncate(time.Minute).UnixMilli()
		if startTime < startTimeThreshold {
			return false
		}
	}

	if timeWindow.End != 0 {
		endTimeThreshold := now.Add(-1 * time.Duration(timeWindow.End).Abs()).Add(1 * time.Minute).Truncate(time.Minute).UnixMilli()
		if endTime > endTimeThreshold {
			return false
		}
	}

	return true
}
