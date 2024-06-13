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

const queryRejectErrorMessage = "Query was rejected by operator. Please contact the support for further questions"

func rejectQueryOrSetPriority(r *http.Request, now time.Time, lookbackDelta time.Duration, limits Limits, userStr string, rejectedQueriesPerTenant *prometheus.CounterVec) error {
	if limits == nil || !(limits.QueryPriority(userStr).Enabled || limits.QueryRejection(userStr).Enabled) {
		return nil
	}

	isExpressionQuery := strings.HasSuffix(r.URL.Path, "/query") || strings.HasSuffix(r.URL.Path, "/query_range")
	isMetadataQuery := strings.HasSuffix(r.URL.Path, "/series") || strings.HasSuffix(r.URL.Path, "/labels") || strings.HasSuffix(r.URL.Path, "/values")

	if isExpressionQuery {
		query := r.FormValue("query")
		expr, err := parser.ParseExpr(query)
		if err != nil {
			return httpgrpc.Errorf(http.StatusBadRequest, err.Error())
		}
		minTime, maxTime := util.FindMinMaxTime(r, expr, lookbackDelta, now)

		if queryReject := limits.QueryRejection(userStr); queryReject.Enabled && query != "" {
			for _, attribute := range queryReject.QueryAttributes {
				if matchAttributeForExpressionQuery(attribute, r, query, expr, now, minTime, maxTime) {
					rejectedQueriesPerTenant.WithLabelValues(userStr).Inc()
					return httpgrpc.Errorf(http.StatusUnprocessableEntity, queryRejectErrorMessage)
				}
			}
		}

		reqStats := stats.FromContext(r.Context())
		reqStats.SetDataSelectMaxTime(maxTime)
		reqStats.SetDataSelectMinTime(minTime)

		if queryPriority := limits.QueryPriority(userStr); queryPriority.Enabled && len(queryPriority.Priorities) != 0 && query != "" {
			for _, priority := range queryPriority.Priorities {
				for _, attribute := range priority.QueryAttributes {
					if matchAttributeForExpressionQuery(attribute, r, query, expr, now, minTime, maxTime) {
						reqStats.SetPriority(priority.Priority)
						return nil
					}
				}
			}
			reqStats.SetPriority(queryPriority.DefaultPriority)
		}
	}

	if queryReject := limits.QueryRejection(userStr); queryReject.Enabled && isMetadataQuery {
		for _, attribute := range queryReject.QueryAttributes {

			if matchAttributeForMetadataQuery(attribute, r, now) {
				rejectedQueriesPerTenant.WithLabelValues(userStr).Inc()
				return httpgrpc.Errorf(http.StatusUnprocessableEntity, queryRejectErrorMessage)
			}
		}
	}

	return nil
}

func matchAttributeForExpressionQuery(attribute validation.QueryAttribute, r *http.Request, query string, expr parser.Expr, now time.Time, minTime, maxTime int64) bool {
	if attribute.Regex != "" && attribute.Regex != ".*" && attribute.Regex != ".+" {
		if attribute.CompiledRegex != nil && !attribute.CompiledRegex.MatchString(query) {
			return false
		}
	}

	if !isWithinTimeAttributes(attribute.TimeWindow, now, minTime, maxTime) {
		return false
	}

	if !isWithinQueryStepLimit(attribute.QueryStepLimit, r, expr) {
		return false
	}

	if attribute.UserAgent != "" && attribute.UserAgent != r.Header.Get("User-Agent") {
		return false
	}

	if attribute.DashboardUID != "" && attribute.DashboardUID != r.Header.Get("X-Dashboard-Uid") {
		return false
	}

	if attribute.PanelID != "" && attribute.PanelID != r.Header.Get("X-Panel-Id") {
		return false
	}

	return true
}

func matchAttributeForMetadataQuery(attribute validation.QueryAttribute, r *http.Request, now time.Time) bool {
	if attribute.Regex != "" && attribute.Regex != ".*" && attribute.Regex != ".+" && attribute.CompiledRegex != nil {
		for _, matcher := range r.Form["match[]"] {
			if attribute.CompiledRegex.MatchString(matcher) {
				continue
			}
			return false
		}
	}

	startTime, _ := util.ParseTime(r.FormValue("start"))
	endTime, _ := util.ParseTime(r.FormValue("end"))

	if !isWithinTimeAttributes(attribute.TimeWindow, now, startTime, endTime) {
		return false
	}

	if attribute.UserAgent != "" && attribute.UserAgent != r.Header.Get("User-Agent") {
		return false
	}

	if attribute.DashboardUID != "" && attribute.DashboardUID != r.Header.Get("X-Dashboard-Uid") {
		return false
	}

	if attribute.PanelID != "" && attribute.PanelID != r.Header.Get("X-Panel-Id") {
		return false
	}

	return true
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

func isWithinQueryStepLimit(queryStepLimit validation.QueryStepLimit, r *http.Request, expr parser.Expr) bool {
	if queryStepLimit.Min == 0 && queryStepLimit.Max == 0 {
		return true
	}

	step, err := util.ParseDurationMs(r.FormValue("step"))
	if err == nil {
		if queryStepLimit.Min != 0 && time.Duration(queryStepLimit.Min).Milliseconds() > step {
			return false
		}

		if queryStepLimit.Max != 0 && time.Duration(queryStepLimit.Max).Milliseconds() < step {
			return false
		}
	}

	var subQueryStep time.Duration
	parser.Inspect(expr, func(node parser.Node, nodes []parser.Node) error {
		e, ok := node.(*parser.SubqueryExpr)
		if ok {
			subQueryStep = e.Step
			return err
		}
		return nil
	})

	if subQueryStep != 0 {
		if queryStepLimit.Min != 0 && time.Duration(queryStepLimit.Min) > subQueryStep {
			return false
		}
		if queryStepLimit.Max != 0 && time.Duration(queryStepLimit.Max) < subQueryStep {
			return false
		}
	}

	return true
}
