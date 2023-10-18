package query

import (
	"net/url"
	"regexp"
	"strconv"
	"time"

	"github.com/cortexproject/cortex/pkg/util/validation"
)

func IsHighPriority(requestParams url.Values, timestamp time.Time, highPriorityQueries []validation.HighPriorityQuery) bool {
	queryParam := requestParams.Get("query")
	timeParam := requestParams.Get("time")
	startParam := requestParams.Get("start")
	endParam := requestParams.Get("end")

	for _, highPriorityQuery := range highPriorityQueries {
		regex := highPriorityQuery.Regex

		if match, err := regexp.MatchString(regex, queryParam); !match || err != nil {
			continue
		}

		startTimeThreshold := timestamp.Add(-1 * highPriorityQuery.StartTime.Abs()).UnixMilli()
		endTimeThreshold := timestamp.Add(-1 * highPriorityQuery.EndTime.Abs()).UnixMilli()

		if instantTime, err := strconv.ParseInt(timeParam, 10, 64); err == nil {
			if isBetweenThresholds(instantTime, instantTime, startTimeThreshold, endTimeThreshold) {
				return true
			}
		}

		if startTime, err := strconv.ParseInt(startParam, 10, 64); err == nil {
			if endTime, err := strconv.ParseInt(endParam, 10, 64); err == nil {
				if isBetweenThresholds(startTime, endTime, startTimeThreshold, endTimeThreshold) {
					return true
				}
			}
		}
	}

	return false
}

func isBetweenThresholds(start, end, startThreshold, endThreshold int64) bool {
	return start >= startThreshold && end <= endThreshold
}
