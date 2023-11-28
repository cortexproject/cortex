package tripperware

import (
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func GetPriority(r *http.Request, userID string, limits Limits, now time.Time) (int64, error) {
	isQuery := strings.HasSuffix(r.URL.Path, "/query")
	isQueryRange := strings.HasSuffix(r.URL.Path, "/query_range")
	queryPriority := limits.QueryPriority(userID)
	query := r.FormValue("query")

	if (!isQuery && !isQueryRange) || !queryPriority.Enabled || query == "" {
		return 0, nil
	}

	expr, err := parser.ParseExpr(query)
	if err != nil {
		return 0, err
	}

	var startTime, endTime int64
	if isQuery {
		if t, err := util.ParseTimeParam(r, "time", now.Unix()); err == nil {
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
		LookbackDelta: limits.MaxQueryLookback(userID), // this is available from querier flag.
	}

	minTime, maxTime := FindMinMaxTime(es)

	for _, priority := range queryPriority.Priorities {
		for _, attribute := range priority.QueryAttributes {
			if attribute.Regex == "" || (attribute.CompiledRegex != nil && !attribute.CompiledRegex.MatchString(query)) {
				continue
			}

			if isWithinTimeAttributes(attribute, now, minTime, maxTime) {
				return priority.Priority, nil
			}
		}
	}

	return queryPriority.DefaultPriority, nil
}

func isWithinTimeAttributes(attribute validation.QueryAttribute, now time.Time, startTime, endTime int64) bool {
	if attribute.StartTime == 0 && attribute.EndTime == 0 {
		return true
	}

	if attribute.StartTime != 0 {
		startTimeThreshold := now.Add(-1 * time.Duration(attribute.StartTime).Abs()).Truncate(time.Second).Unix()
		if startTime < startTimeThreshold {
			return false
		}
	}

	if attribute.EndTime != 0 {
		endTimeThreshold := now.Add(-1 * time.Duration(attribute.EndTime).Abs()).Add(1 * time.Second).Truncate(time.Second).Unix()
		if endTime > endTimeThreshold {
			return false
		}
	}

	return true
}

func FindMinMaxTime(s *parser.EvalStmt) (int64, int64) {
	// Placeholder until Prometheus is updated to include
	// https://github.com/prometheus/prometheus/commit/9e3df532d8294d4fe3284bde7bc96db336a33552
	return s.Start.Unix(), s.End.Unix()
}
