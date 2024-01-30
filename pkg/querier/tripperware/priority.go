package tripperware

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

var (
	errParseExpr = errors.New("failed to parse expr")
)

func GetPriority(r *http.Request, userID string, limits Limits, now time.Time, lookbackDelta time.Duration) (int64, error) {
	isQuery := strings.HasSuffix(r.URL.Path, "/query")
	isQueryRange := strings.HasSuffix(r.URL.Path, "/query_range")
	queryPriority := limits.QueryPriority(userID)
	query := r.FormValue("query")

	if (!isQuery && !isQueryRange) || !queryPriority.Enabled || query == "" {
		return 0, nil
	}

	expr, err := parser.ParseExpr(query)
	if err != nil {
		// If query fails to be parsed, we throw a simple parse error
		// and fail query later on querier.
		return 0, errParseExpr
	}

	if len(queryPriority.Priorities) == 0 {
		return queryPriority.DefaultPriority, nil
	}

	var startTime, endTime int64
	if isQuery {
		if t, err := util.ParseTimeParam(r, "time", now.UnixMilli()); err == nil {
			startTime = t
			endTime = t
		}
	} else if isQueryRange {
		if st, err := util.ParseTime(r.FormValue("start")); err == nil {
			if et, err := util.ParseTime(r.FormValue("end")); err == nil {
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

	minTime, maxTime := promql.FindMinMaxTime(es)

	for _, priority := range queryPriority.Priorities {
		for _, attribute := range priority.QueryAttributes {
			if attribute.Regex != "" && attribute.Regex != ".*" && attribute.Regex != ".+" {
				if attribute.CompiledRegex != nil && !attribute.CompiledRegex.MatchString(query) {
					continue
				}
			}

			if isWithinTimeAttributes(attribute.TimeWindow, now, minTime, maxTime) {
				return priority.Priority, nil
			}
		}
	}

	return queryPriority.DefaultPriority, nil
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
