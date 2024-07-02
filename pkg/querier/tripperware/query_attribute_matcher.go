package tripperware

import (
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/querier/stats"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

const QueryRejectErrorMessage = "This query has been rejected by the service operator."

func rejectQueryOrSetPriority(r *http.Request, now time.Time, lookbackDelta time.Duration, limits Limits, userStr string, rejectedQueriesPerTenant *prometheus.CounterVec) error {
	if limits == nil || !(limits.QueryPriority(userStr).Enabled || limits.QueryRejection(userStr).Enabled) {
		return nil
	}
	op := getOperation(r)

	if op == "query" || op == "query_range" {
		query := r.FormValue("query")
		expr, err := parser.ParseExpr(query)
		if err != nil {
			return httpgrpc.Errorf(http.StatusBadRequest, err.Error())
		}
		minTime, maxTime := util.FindMinMaxTime(r, expr, lookbackDelta, now)

		if queryReject := limits.QueryRejection(userStr); queryReject.Enabled && query != "" {
			for _, attribute := range queryReject.QueryAttributes {
				if matchAttributeForExpressionQuery(attribute, op, r, query, now, minTime, maxTime) {
					rejectedQueriesPerTenant.WithLabelValues(op, userStr).Inc()
					return httpgrpc.Errorf(http.StatusUnprocessableEntity, QueryRejectErrorMessage)
				}
			}
		}

		reqStats := stats.FromContext(r.Context())
		reqStats.SetDataSelectMaxTime(maxTime)
		reqStats.SetDataSelectMinTime(minTime)

		if queryPriority := limits.QueryPriority(userStr); queryPriority.Enabled && len(queryPriority.Priorities) != 0 && query != "" {
			for _, priority := range queryPriority.Priorities {
				for _, attribute := range priority.QueryAttributes {
					if matchAttributeForExpressionQuery(attribute, op, r, query, now, minTime, maxTime) {
						reqStats.SetPriority(priority.Priority)
						return nil
					}
				}
			}
			reqStats.SetPriority(queryPriority.DefaultPriority)
		}
	}

	if queryReject := limits.QueryRejection(userStr); queryReject.Enabled && (op == "series" || op == "labels" || op == "label_values") {
		for _, attribute := range queryReject.QueryAttributes {
			if matchAttributeForMetadataQuery(attribute, op, r, now) {
				rejectedQueriesPerTenant.WithLabelValues(op, userStr).Inc()
				return httpgrpc.Errorf(http.StatusUnprocessableEntity, QueryRejectErrorMessage)
			}
		}
	}

	return nil
}

func getOperation(r *http.Request) string {
	switch {
	case strings.HasSuffix(r.URL.Path, "/query"):
		return "query"
	case strings.HasSuffix(r.URL.Path, "/query_range"):
		return "query_range"
	case strings.HasSuffix(r.URL.Path, "/series"):
		return "series"
	case strings.HasSuffix(r.URL.Path, "/labels"):
		return "labels"
	case strings.HasSuffix(r.URL.Path, "/values"):
		return "label_values"
	default:
		return "other"
	}
}

func matchAttributeForExpressionQuery(attribute validation.QueryAttribute, op string, r *http.Request, query string, now time.Time, minTime, maxTime int64) bool {
	if attribute.ApiType != "" && attribute.ApiType != op {
		return false
	}
	if attribute.Regex != "" && attribute.Regex != ".*" && attribute.Regex != ".+" {
		if attribute.CompiledRegex != nil && !attribute.CompiledRegex.MatchString(query) {
			return false
		}
	}

	if !isWithinTimeAttributes(attribute.TimeWindow, now, minTime, maxTime) {
		return false
	}

	if !isWithinTimeRangeAttribute(attribute.TimeRangeLimit, minTime, maxTime) {
		return false
	}

	if op == "query_range" && !isWithinQueryStepLimit(attribute.QueryStepLimit, r) {
		return false
	}

	if attribute.UserAgentRegex != "" && attribute.UserAgentRegex != ".*" && attribute.CompiledUserAgentRegex != nil {
		if !attribute.CompiledUserAgentRegex.MatchString(r.Header.Get("User-Agent")) {
			return false
		}
	}

	if attribute.DashboardUID != "" && attribute.DashboardUID != r.Header.Get("X-Dashboard-Uid") {
		return false
	}

	if attribute.PanelID != "" && attribute.PanelID != r.Header.Get("X-Panel-Id") {
		return false
	}

	return true
}

func matchAttributeForMetadataQuery(attribute validation.QueryAttribute, op string, r *http.Request, now time.Time) bool {
	if attribute.ApiType != "" && attribute.ApiType != op {
		return false
	}
	if err := r.ParseForm(); err != nil {
		return false
	}
	if attribute.Regex != "" && attribute.Regex != ".*" && attribute.CompiledRegex != nil {
		atLeastOneMatched := false
		for _, matcher := range r.Form["match[]"] {
			if attribute.CompiledRegex.MatchString(matcher) {
				atLeastOneMatched = true
				break
			}
		}
		if !atLeastOneMatched {
			return false
		}
	}

	startTime, _ := util.ParseTime(r.FormValue("start"))
	endTime, _ := util.ParseTime(r.FormValue("end"))

	if !isWithinTimeAttributes(attribute.TimeWindow, now, startTime, endTime) {
		return false
	}

	if !isWithinTimeRangeAttribute(attribute.TimeRangeLimit, startTime, endTime) {
		return false
	}

	if attribute.UserAgentRegex != "" && attribute.UserAgentRegex != ".*" && attribute.CompiledUserAgentRegex != nil {
		if !attribute.CompiledUserAgentRegex.MatchString(r.Header.Get("User-Agent")) {
			return false
		}
	}

	return true
}

func isWithinTimeAttributes(timeWindow validation.TimeWindow, now time.Time, startTime, endTime int64) bool {
	if timeWindow.Start == 0 && timeWindow.End == 0 {
		return true
	}

	if timeWindow.Start != 0 {
		startTimeThreshold := now.Add(-1 * time.Duration(timeWindow.Start).Abs()).Add(-1 * time.Minute).Truncate(time.Minute).UnixMilli()
		if startTime == 0 || startTime < startTimeThreshold {
			return false
		}
	}

	if timeWindow.End != 0 {
		endTimeThreshold := now.Add(-1 * time.Duration(timeWindow.End).Abs()).Add(1 * time.Minute).Truncate(time.Minute).UnixMilli()
		if endTime == 0 || endTime > endTimeThreshold {
			return false
		}
	}

	return true
}

func isWithinTimeRangeAttribute(limit validation.TimeRangeLimit, startTime, endTime int64) bool {
	if limit.Min == 0 && limit.Max == 0 {
		return true
	}

	if startTime == 0 || endTime == 0 {
		return false
	}

	timeRangeInMillis := endTime - startTime

	if limit.Min != 0 && time.Duration(limit.Min).Milliseconds() > timeRangeInMillis {
		return false
	}
	if limit.Max != 0 && time.Duration(limit.Max).Milliseconds() < timeRangeInMillis {
		return false
	}

	return true
}

func isWithinQueryStepLimit(queryStepLimit validation.QueryStepLimit, r *http.Request) bool {
	if queryStepLimit.Min == 0 && queryStepLimit.Max == 0 {
		return true
	}

	step, err := util.ParseDurationMs(r.FormValue("step"))
	if err != nil {
		return false
	}

	if queryStepLimit.Min != 0 && time.Duration(queryStepLimit.Min).Milliseconds() > step {
		return false
	}
	if queryStepLimit.Max != 0 && time.Duration(queryStepLimit.Max).Milliseconds() < step {
		return false
	}

	return true
}
