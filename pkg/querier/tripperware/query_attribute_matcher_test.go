package tripperware

import (
	"context"
	"net/http"
	"net/url"
	"regexp"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/querier/stats"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"

	"github.com/cortexproject/cortex/pkg/util/validation"
)

var rejectedQueriesPerTenant = prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"user"})

func Test_rejectQueryOrSetPriorityShouldReturnDefaultPriorityIfNotEnabledOrInvalidQueryString(t *testing.T) {

	limits := mockLimits{queryPriority: validation.QueryPriority{
		Priorities: []validation.PriorityDef{
			{
				Priority: 1,
				QueryAttributes: []validation.QueryAttribute{
					{
						Regex:         ".*",
						CompiledRegex: regexp.MustCompile(".*"),
					},
				},
			},
		},
	},
		queryRejection: validation.QueryRejection{
			Enabled: false,
		},
	}

	type testCase struct {
		queryPriorityEnabled  bool
		queryRejectionEnabled bool
		path                  string
		expectedError         error
		expectedPriority      int64
	}

	tests := map[string]testCase{
		"should miss if query priority/rejection not enabled": {
			path: "/api/v1/query?time=1536716898&query=sum%28container_memory_rss%29+by+%28namespace%29",
		},
		"should throw parse error if query string empty": {
			queryPriorityEnabled:  true,
			queryRejectionEnabled: true,
			path:                  "/api/v1/query?time=1536716898&query=",
			expectedError:         httpgrpc.Errorf(http.StatusBadRequest, "unknown position: parse error: no expression found in input"),
		},
		"should miss if it's metadata query and only priority is enabled": {
			queryPriorityEnabled:  true,
			queryRejectionEnabled: false,
			path:                  "/api/v1/labels?match[]",
		},
		"should set priority if regex match and rejection disabled": {
			queryPriorityEnabled:  true,
			queryRejectionEnabled: false,
			path:                  "/api/v1/query?time=1536716898&query=sum%28container_memory_rss%29+by+%28namespace%29",
			expectedPriority:      int64(1),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			req, err := http.NewRequest("GET", testData.path, http.NoBody)
			require.NoError(t, err)
			reqStats, ctx := stats.ContextWithEmptyStats(context.Background())
			req = req.WithContext(ctx)
			limits.queryPriority.Enabled = testData.queryPriorityEnabled
			resultErr := rejectQueryOrSetPriority(req, time.Now(), time.Duration(1), limits, "", rejectedQueriesPerTenant)
			assert.Equal(t, testData.expectedError, resultErr)
			assert.Equal(t, testData.expectedPriority, reqStats.Priority)
		})
	}
}

func Test_matchAttributeForExpressionQueryShouldMatchRegex(t *testing.T) {
	queryAttribute := validation.QueryAttribute{}

	type testCase struct {
		regex  string
		query  string
		result bool
	}

	tests := map[string]testCase{
		"should hit if regex matches": {
			regex:  "(^sum|c(.+)t)",
			query:  "sum(up)",
			result: true,
		},
		"should miss if regex doesn't match": {
			regex: "(^sum|c(.+)t)",
			query: "min(up)",
		},
		"should hit if regex matches - .*": {
			regex:  ".*",
			query:  "count(sum(up))",
			result: true,
		},
		"should hit if regex matches - .+": {
			regex:  ".+",
			query:  "count(sum(up))",
			result: true,
		},
		"should hit if regex is an empty string": {
			regex:  "",
			query:  "sum(up)",
			result: true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			queryAttribute.Regex = testData.regex
			queryAttribute.CompiledRegex = regexp.MustCompile(testData.regex)
			expr, err := parser.ParseExpr(testData.query)
			require.NoError(t, err)
			priority := matchAttributeForExpressionQuery(queryAttribute, &http.Request{}, testData.query, expr, time.Time{}, 0, 0)
			assert.Equal(t, testData.result, priority)
		})
	}

}

func Test_isWithinTimeAttributes(t *testing.T) {
	now := time.Now()

	timeWindow := validation.TimeWindow{
		Start: model.Duration(45 * time.Minute),
		End:   model.Duration(15 * time.Minute),
	}

	type testCase struct {
		timeWindow     validation.TimeWindow
		start          time.Time
		end            time.Time
		expectedResult bool
	}

	tests := map[string]testCase{
		"should hit between start and end time": {
			timeWindow:     timeWindow,
			start:          now.Add(-40 * time.Minute),
			end:            now.Add(-20 * time.Minute),
			expectedResult: true,
		},
		"should hit equal to start and end time": {
			timeWindow:     timeWindow,
			start:          now.Add(-45 * time.Minute),
			end:            now.Add(-15 * time.Minute),
			expectedResult: true,
		},
		"should miss outside of start time": {
			timeWindow:     timeWindow,
			start:          now.Add(-50 * time.Minute),
			end:            now.Add(-15 * time.Minute),
			expectedResult: false,
		},
		"should miss completely outside of start time": {
			timeWindow:     timeWindow,
			start:          now.Add(-50 * time.Minute),
			end:            now.Add(-45 * time.Minute),
			expectedResult: false,
		},
		"should miss outside of end time": {
			timeWindow:     timeWindow,
			start:          now.Add(-45 * time.Minute),
			end:            now.Add(-10 * time.Minute),
			expectedResult: false,
		},
		"should miss completely outside of end time": {
			timeWindow:     timeWindow,
			start:          now.Add(-15 * time.Minute),
			end:            now.Add(-10 * time.Minute),
			expectedResult: false,
		},
		"should not consider on empty start and end time": {
			start:          now.Add(-15 * time.Minute),
			end:            now.Add(-10 * time.Minute),
			expectedResult: true,
		},
		"should not consider start on empty start": {
			timeWindow: validation.TimeWindow{
				End: model.Duration(15 * time.Minute),
			},
			start:          now.Add(-50 * time.Minute),
			end:            now.Add(-20 * time.Minute),
			expectedResult: true,
		},
		"should not consider end on empty end time": {
			timeWindow: validation.TimeWindow{
				Start: model.Duration(45 * time.Minute),
			},
			start:          now.Add(-40 * time.Minute),
			end:            now.Add(-10 * time.Minute),
			expectedResult: true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			priority := isWithinTimeAttributes(testData.timeWindow, now, testData.start.UnixMilli(), testData.end.UnixMilli())
			assert.Equal(t, testData.expectedResult, priority)
		})
	}
}

func Test_isWithinQueryStepLimit(t *testing.T) {

	queryStepLimit := validation.QueryStepLimit{
		Min: model.Duration(time.Second * 5),
		Max: model.Duration(time.Minute * 2),
	}

	type testCase struct {
		step           string
		queryString    string
		queryStepLimit validation.QueryStepLimit
		expectedResult bool
	}

	tests := map[string]testCase{
		"query within the limits if no limits was defined for steps": {
			queryString:    "count(sum(up))",
			step:           "15s",
			expectedResult: true,
		},
		"query should be considered outside of the step limit if query doesn't have steps": {
			queryString:    "count(sum(up))",
			step:           "not_parseable",
			queryStepLimit: queryStepLimit,
		},
		"should match if step limit set and step is within the range": {
			step:           "15s",
			queryString:    "count(sum(up))",
			queryStepLimit: queryStepLimit,
			expectedResult: true,
		},
		"should not match if min step limit set and step is size is smaller": {
			step:           "4s",
			queryString:    "count(sum(up))",
			queryStepLimit: queryStepLimit,
			expectedResult: false,
		},
		"should not match if max step limit set and step is size is bigger": {
			step:           "3m",
			queryString:    "count(sum(up))",
			queryStepLimit: queryStepLimit,
			expectedResult: false,
		},
		"should not match if step limit set and subquery step is not within the range": {
			step:           "15s",
			queryString:    "up[60m:2s]",
			queryStepLimit: queryStepLimit,
			expectedResult: false,
		},
		"should match if step limit set and both query step and subquery step is within the range": {
			step:           "15s",
			queryString:    "up[60m:1m]",
			queryStepLimit: queryStepLimit,
			expectedResult: true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			params := url.Values{}
			params.Add("step", testData.step)
			req, err := http.NewRequest("POST", "/query?"+params.Encode(), http.NoBody)
			require.NoError(t, err)

			expr, err := parser.ParseExpr(testData.queryString)
			require.NoError(t, err)

			result := isWithinQueryStepLimit(testData.queryStepLimit, req, expr)
			assert.Equal(t, testData.expectedResult, result)
		})
	}
}

func Test_matchAttributeForMetadataQueryHeadersShouldBeCheckedIfSet(t *testing.T) {

	type testCase struct {
		headers        http.Header
		queryAttribute validation.QueryAttribute
		expectedResult bool
	}

	tests := map[string]testCase{
		"should not check any of them if attributes are empty (match)": {
			expectedResult: true,
		},
		"should not check if attributes are empty even corresponding headers exist (match)": {
			headers: http.Header{
				"User-Agent":      {"grafana"},
				"X-Dashboard-Uid": {"dashboard-uid"},
				"X-Panel-Id":      {"panel-id"},
			},
			expectedResult: true,
		},
		"should match all attributes if all set and all headers provided": {
			headers: http.Header{
				"User-Agent":      {"grafana"},
				"X-Dashboard-Uid": {"dashboard-uid"},
				"X-Panel-Id":      {"panel-id"},
			},
			queryAttribute: validation.QueryAttribute{
				UserAgent:    "grafana",
				DashboardUID: "dashboard-uid",
				PanelID:      "panel-id",
			},
			expectedResult: true,
		},
		"should not match if headers are missing for provided attributes ": {
			headers: http.Header{
				"X-Dashboard-Uid": {"dashboard-uid"},
			},
			queryAttribute: validation.QueryAttribute{
				UserAgent:    "grafana",
				DashboardUID: "dashboard-uid",
				PanelID:      "panel-id",
			},
		},
		"should not match if both attribute and header is set but does not match ": {
			headers: http.Header{
				"User-Agent": {"python"},
			},
			queryAttribute: validation.QueryAttribute{
				UserAgent: "grafana",
			},
		},
		"should not compare if values are empty (match)": {
			headers: http.Header{
				"X-Panel-Id": {""},
			},
			queryAttribute: validation.QueryAttribute{
				PanelID: "",
			},
			expectedResult: true,
		},
		"should match if headers match provided attributes ": {
			headers: http.Header{
				"User-Agent": {"python"},
				"X-Panel-Id": {"pane"},
			},
			queryAttribute: validation.QueryAttribute{
				PanelID: "pane",
			},
			expectedResult: true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			req, err := http.NewRequest("GET", "/", http.NoBody)
			require.NoError(t, err)
			req.Header = testData.headers

			result := matchAttributeForMetadataQuery(testData.queryAttribute, req, time.Now())
			assert.Equal(t, testData.expectedResult, result)
		})
	}
}
