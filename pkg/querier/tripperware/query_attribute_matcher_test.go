package tripperware

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/querier/stats"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

var rejectedQueriesPerTenant = prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"op", "user"})

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
			limits.queryRejection.Enabled = testData.queryRejectionEnabled
			resultErr := rejectQueryOrSetPriority(req, time.Now(), time.Duration(1), limits, "", rejectedQueriesPerTenant)
			assert.Equal(t, testData.expectedError, resultErr)
			assert.Equal(t, testData.expectedPriority, reqStats.Priority)
		})
	}
}

func Test_rejectQueryOrSetPriorityShouldRejectIfMatches(t *testing.T) {
	now := time.Now()
	limits := mockLimits{
		queryRejection: validation.QueryRejection{
			Enabled:         false,
			QueryAttributes: []validation.QueryAttribute{},
		},
	}

	type testCase struct {
		queryRejectionEnabled bool
		path                  string
		expectedError         error
		expectedPriority      int64
		rejectQueryAttribute  validation.QueryAttribute
	}

	tests := map[string]testCase{

		"should not reject if query rejection not enabled": {
			queryRejectionEnabled: false,
			path:                  "/api/v1/query_range?start=1536716898&end=1536729898&step=7s&query=avg_over_time%28rate%28node_cpu_seconds_total%5B1m%5D%29%5B10m%3A5s%5D%29",
			expectedError:         nil,
			rejectQueryAttribute: validation.QueryAttribute{
				Regex:         ".*",
				CompiledRegex: regexp.MustCompile(".*"),
			},
		},

		"should reject if query rejection enabled with all query match regex": {
			queryRejectionEnabled: true,
			path:                  "/api/v1/query_range?start=1536716898&end=1536729898&step=7s&query=avg_over_time%28rate%28node_cpu_seconds_total%5B1m%5D%29%5B10m%3A5s%5D%29",
			expectedError:         httpgrpc.Errorf(http.StatusUnprocessableEntity, QueryRejectErrorMessage),
			rejectQueryAttribute: validation.QueryAttribute{
				Regex:         ".*",
				CompiledRegex: regexp.MustCompile(".*"),
			},
		},

		"should reject if query rejection enabled with step limit and query match": {
			queryRejectionEnabled: true,
			path:                  "/api/v1/query_range?start=1536716898&end=1536729898&step=7s&query=count%28sum%28up%29%29", //count(sum(up))
			expectedError:         httpgrpc.Errorf(http.StatusUnprocessableEntity, QueryRejectErrorMessage),
			rejectQueryAttribute: validation.QueryAttribute{
				QueryStepLimit: validation.QueryStepLimit{
					Min: model.Duration(time.Second * 5),
					Max: model.Duration(time.Minute * 2),
				},
			},
		},

		"should reject if query rejection enabled with min step limit and query match": {
			queryRejectionEnabled: true,
			path:                  "/api/v1/query_range?start=1536716898&end=1536729898&step=7m&query=count%28sum%28up%29%29", //count(sum(up))
			expectedError:         httpgrpc.Errorf(http.StatusUnprocessableEntity, QueryRejectErrorMessage),
			rejectQueryAttribute: validation.QueryAttribute{
				QueryStepLimit: validation.QueryStepLimit{
					Min: model.Duration(time.Minute * 5),
				},
			},
		},

		"should reject if query rejection enabled with step limit and subQuery step match": {
			queryRejectionEnabled: true,
			path:                  "/api/v1/query?time=1536716898&query=avg_over_time%28rate%28node_cpu_seconds_total%5B1m%5D%29%5B10m%3A5s%5D%29", //avg_over_time(rate(node_cpu_seconds_total[1m])[10m:5s])
			expectedError:         httpgrpc.Errorf(http.StatusUnprocessableEntity, QueryRejectErrorMessage),
			rejectQueryAttribute: validation.QueryAttribute{
				QueryStepLimit: validation.QueryStepLimit{
					Min: model.Duration(time.Second * 5),
					Max: model.Duration(time.Minute * 2),
				},
			},
		},

		"should ignore step limit for instant query, and reject if other properties of query_attribute matches": {
			queryRejectionEnabled: true,
			path:                  "/api/v1/query?time=1536716898&query=avg_over_time%28rate%28node_cpu_seconds_total%5B1m%5D%29%5B10m%3A5s%5D%29", //avg_over_time(rate(node_cpu_seconds_total[1m])[10m:5s])
			expectedError:         httpgrpc.Errorf(http.StatusUnprocessableEntity, QueryRejectErrorMessage),
			rejectQueryAttribute: validation.QueryAttribute{
				Regex: ".*over_time.*",
				QueryStepLimit: validation.QueryStepLimit{
					Min: model.Duration(time.Second * 6),
					Max: model.Duration(time.Minute * 2),
				},
			},
		},

		"should reject if query rejection enabled with time window matching": {
			queryRejectionEnabled: true,
			path:                  fmt.Sprintf("/api/v1/query_range?start=%d&end=%d&step=7s&query=%s", now.Add(-30*time.Minute).UnixMilli()/1000, now.Add(-20*time.Minute).UnixMilli()/1000, url.QueryEscape("count(sum(up))")),
			expectedError:         httpgrpc.Errorf(http.StatusUnprocessableEntity, QueryRejectErrorMessage),
			rejectQueryAttribute: validation.QueryAttribute{
				TimeWindow: validation.TimeWindow{
					Start: model.Duration(45 * time.Minute),
					End:   model.Duration(15 * time.Minute),
				},
			},
		},

		"should reject if query rejection api matches and regex matches match[] of series request and time window": {
			queryRejectionEnabled: true,
			path:                  fmt.Sprintf("/api/v1/series?start=%d&end=%d&step=7s&match[]=%s", now.Add(-30*time.Minute).UnixMilli()/1000, now.Add(-20*time.Minute).UnixMilli()/1000, url.QueryEscape("count(sum(up))")),
			expectedError:         httpgrpc.Errorf(http.StatusUnprocessableEntity, QueryRejectErrorMessage),
			rejectQueryAttribute: validation.QueryAttribute{
				ApiType:       "series",
				Regex:         ".*sum.*",
				CompiledRegex: regexp.MustCompile(".*sum.*"),
				TimeWindow: validation.TimeWindow{
					Start: model.Duration(45 * time.Minute),
					End:   model.Duration(15 * time.Minute),
				},
			},
		},

		"should not reject if query api_type doesn't match matches": {
			queryRejectionEnabled: true,
			path:                  fmt.Sprintf("/api/v1/series?start=%d&end=%d&step=7s&match[]=%s", now.Add(-30*time.Minute).UnixMilli()/1000, now.Add(-20*time.Minute).UnixMilli()/1000, url.QueryEscape("count(sum(up))")),
			expectedError:         nil,
			rejectQueryAttribute: validation.QueryAttribute{
				ApiType:       "query",
				Regex:         ".*sum.*",
				CompiledRegex: regexp.MustCompile(".*sum.*"),
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			req, err := http.NewRequest("GET", testData.path, http.NoBody)
			require.NoError(t, err)
			reqStats, ctx := stats.ContextWithEmptyStats(context.Background())
			req = req.WithContext(ctx)
			limits.queryRejection.Enabled = testData.queryRejectionEnabled
			limits.queryRejection.QueryAttributes = []validation.QueryAttribute{testData.rejectQueryAttribute}
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
			priority := matchAttributeForExpressionQuery(queryAttribute, "query_range", &http.Request{}, testData.query, time.Time{}, 0, 0)
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
		"should not consider start on empty start limit": {
			timeWindow: validation.TimeWindow{
				End: model.Duration(15 * time.Minute),
			},
			start:          now.Add(-50 * time.Minute),
			end:            now.Add(-20 * time.Minute),
			expectedResult: true,
		},
		"should not consider end on empty end limit": {
			timeWindow: validation.TimeWindow{
				Start: model.Duration(45 * time.Minute),
			},
			start:          now.Add(-40 * time.Minute),
			end:            now.Add(-10 * time.Minute),
			expectedResult: true,
		},
		"should miss if start time for query is missing but start for limits exists": {
			timeWindow: validation.TimeWindow{
				Start: model.Duration(45 * time.Minute),
				End:   model.Duration(15 * time.Minute),
			},
			start:          time.UnixMilli(0),
			end:            now.Add(-20 * time.Minute),
			expectedResult: false,
		},
		"should miss if end time for query is missing but end for limits exists": {
			timeWindow: validation.TimeWindow{
				Start: model.Duration(45 * time.Minute),
				End:   model.Duration(15 * time.Minute),
			},
			start:          now.Add(-40 * time.Minute),
			end:            time.UnixMilli(0),
			expectedResult: false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			priority := isWithinTimeAttributes(testData.timeWindow, now, testData.start.UnixMilli(), testData.end.UnixMilli())
			assert.Equal(t, testData.expectedResult, priority)
		})
	}
}

func Test_isWithinTimeRangeAttribute(t *testing.T) {
	now := time.Now().UnixMilli()

	timeRangeLimit := validation.TimeRangeLimit{
		Min: model.Duration(12 * time.Hour),
		Max: model.Duration(15 * 24 * time.Hour),
	}

	type testCase struct {
		timeRangeLimit validation.TimeRangeLimit
		startTime      int64
		endTime        int64
		expectedResult bool
	}

	tests := map[string]testCase{
		"valid if within timeRange": {
			timeRangeLimit: timeRangeLimit,
			startTime:      now - 20*time.Hour.Milliseconds(),
			endTime:        now,
			expectedResult: true,
		},
		"valid if queryTimeRange is equal to the limit": {
			timeRangeLimit: timeRangeLimit,
			startTime:      now - 12*time.Hour.Milliseconds(),
			endTime:        now,
			expectedResult: true,
		},
		"not valid if queryTimeRange is smaller than the limit min": {
			timeRangeLimit: timeRangeLimit,
			startTime:      now - 11*time.Hour.Milliseconds(),
			endTime:        now,
			expectedResult: false,
		},
		"not valid if queryTimeRange is bigger than the limit max": {
			timeRangeLimit: timeRangeLimit,
			startTime:      now - 35*24*time.Hour.Milliseconds(),
			endTime:        now,
			expectedResult: false,
		},
		"valid if max is not provided and queryRange is bigger than min": {
			timeRangeLimit: validation.TimeRangeLimit{
				Min: model.Duration(12 * time.Hour),
			},
			startTime:      now - 35*24*time.Hour.Milliseconds(),
			endTime:        now,
			expectedResult: true,
		},
		"valid if min is not provided and queryRange is smaller than max": {
			timeRangeLimit: validation.TimeRangeLimit{
				Max: model.Duration(15 * 24 * time.Hour),
			},
			startTime:      now - 14*24*time.Hour.Milliseconds(),
			endTime:        now,
			expectedResult: true,
		},
		"not valid if limit provided but query doesn't have range (missing startTime or endTime)": {
			timeRangeLimit: validation.TimeRangeLimit{
				Max: model.Duration(15 * 24 * time.Hour),
			},
			startTime:      now,
			expectedResult: false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			priority := isWithinTimeRangeAttribute(testData.timeRangeLimit, testData.startTime, testData.endTime)
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
		"should match if step limit set is within the range and should ignore subquery step even it's outside the range": {
			step:           "15s",
			queryString:    "up[60m:5m]",
			queryStepLimit: queryStepLimit,
			expectedResult: true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			params := url.Values{}
			if testData.step != "" {
				params.Add("step", testData.step)
			}
			req, err := http.NewRequest("POST", "/query?"+params.Encode(), http.NoBody)
			require.NoError(t, err)

			require.NoError(t, err)

			result := isWithinQueryStepLimit(testData.queryStepLimit, req)
			assert.Equal(t, testData.expectedResult, result)
		})
	}
}

func Test_matchAttributeForExpressionQueryHeadersShouldBeCheckedIfSet(t *testing.T) {

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
				"X-Dashboard-Uid": {"dashboard-uid"},
				"X-Panel-Id":      {"panel-id"},
			},
			expectedResult: true,
		},
		"should match all attributes if all set and all headers provided": {
			headers: http.Header{
				"X-Dashboard-Uid": {"dashboard-uid"},
				"X-Panel-Id":      {"panel-id"},
			},
			queryAttribute: validation.QueryAttribute{
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
				DashboardUID: "dashboard-uid",
				PanelID:      "panel-id",
			},
		},
		"should not match if both attribute and header is set but does not match ": {
			headers: http.Header{
				"X-Panel-Id": {"panel123"},
			},
			queryAttribute: validation.QueryAttribute{
				PanelID: "panel-id",
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
				"X-Dashboard-Uid": {"dashboard-uid"},
				"X-Panel-Id":      {"pane"},
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

			result := matchAttributeForExpressionQuery(testData.queryAttribute, "query_range", req, "", time.Time{}, 0, 0)
			assert.Equal(t, testData.expectedResult, result)
		})
	}
}

func Test_matchAttributeForExpressionQueryShouldMatchUserAgentRegex(t *testing.T) {

	type testCase struct {
		userAgentRegex  string
		userAgentHeader string
		result          bool
	}

	tests := map[string]testCase{
		"should hit if regex matches": {
			userAgentRegex:  "(^grafana-agent|prometheus-(.*)client(.+))",
			userAgentHeader: "prometheus-client-go/v0.9.3",
			result:          true,
		},
		"should miss if regex doesn't match": {
			userAgentRegex:  "(^grafana-agent|prometheus-(.*)client(.+))",
			userAgentHeader: "loki",
		},
		"should hit if regex matches - .*": {
			userAgentRegex:  ".*",
			userAgentHeader: "grafana-agent/v0.19.0",
			result:          true,
		},
		"should hit if regex is an empty string": {
			userAgentRegex:  "",
			userAgentHeader: "grafana-agent/v0.19.0",
			result:          true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			req, err := http.NewRequest("GET", "/", http.NoBody)
			require.NoError(t, err)
			req.Header = http.Header{
				"User-Agent": {testData.userAgentHeader},
			}
			queryAttribute := validation.QueryAttribute{
				UserAgentRegex:         testData.userAgentRegex,
				CompiledUserAgentRegex: regexp.MustCompile(testData.userAgentRegex),
			}

			result := matchAttributeForExpressionQuery(queryAttribute, "query_range", req, "", time.Time{}, 0, 0)
			assert.Equal(t, testData.result, result)
		})
	}

}
