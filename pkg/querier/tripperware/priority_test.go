package tripperware

import (
	"bytes"
	"net/http"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"

	"github.com/cortexproject/cortex/pkg/util/validation"
)

func Test_GetPriorityShouldReturnDefaultPriorityIfNotEnabledOrInvalidQueryString(t *testing.T) {
	now := time.Now()
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
	}}

	type testCase struct {
		url                  string
		queryPriorityEnabled bool
		err                  error
	}

	tests := map[string]testCase{
		"should miss if query priority not enabled": {
			url: "/query?query=up",
		},
		"should miss if query string empty": {
			url:                  "/query?query=",
			queryPriorityEnabled: true,
		},
		"shouldn't return error if query is invalid": {
			url:                  "/query?query=up[4h",
			queryPriorityEnabled: true,
			err:                  errParseExpr,
		},
		"should miss if query string empty - range query": {
			url:                  "/query_range?query=",
			queryPriorityEnabled: true,
		},
		"shouldn't return error if query is invalid, range query": {
			url:                  "/query_range?query=up[4h",
			queryPriorityEnabled: true,
			err:                  errParseExpr,
		},
		"should miss if neither instant nor range query": {
			url:                  "/series",
			queryPriorityEnabled: true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			limits.queryPriority.Enabled = testData.queryPriorityEnabled
			req, _ := http.NewRequest(http.MethodPost, testData.url, bytes.NewReader([]byte{}))
			priority, err := GetPriority(req, "", limits, now, 0)
			if err != nil {
				assert.Equal(t, testData.err, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, int64(0), priority)
		})
	}
}

func Test_GetPriorityShouldConsiderRegex(t *testing.T) {
	now := time.Now()
	limits := mockLimits{queryPriority: validation.QueryPriority{
		Enabled: true,
		Priorities: []validation.PriorityDef{
			{
				Priority: 1,
				QueryAttributes: []validation.QueryAttribute{
					{},
				},
			},
		},
	}}

	type testCase struct {
		regex            string
		query            string
		expectedPriority int
	}

	tests := map[string]testCase{
		"should hit if regex matches": {
			regex:            "(^sum|c(.+)t)",
			query:            "sum(up)",
			expectedPriority: 1,
		},
		"should miss if regex doesn't match": {
			regex:            "(^sum|c(.+)t)",
			query:            "min(up)",
			expectedPriority: 0,
		},
		"should hit if regex matches - .*": {
			regex:            ".*",
			query:            "count(sum(up))",
			expectedPriority: 1,
		},
		"should hit if regex matches - .+": {
			regex:            ".+",
			query:            "count(sum(up))",
			expectedPriority: 1,
		},
		"should hit if regex is an empty string": {
			regex:            "",
			query:            "sum(up)",
			expectedPriority: 1,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			limits.queryPriority.Priorities[0].QueryAttributes[0].Regex = testData.regex
			limits.queryPriority.Priorities[0].QueryAttributes[0].CompiledRegex = regexp.MustCompile(testData.regex)
			req, _ := http.NewRequest(http.MethodPost, "/query?query="+testData.query, bytes.NewReader([]byte{}))
			priority, err := GetPriority(req, "", limits, now, 0)
			assert.NoError(t, err)
			assert.Equal(t, int64(testData.expectedPriority), priority)
		})
	}
}

func Test_GetPriorityShouldConsiderStartAndEndTime(t *testing.T) {
	now := time.Now()
	limits := mockLimits{queryPriority: validation.QueryPriority{
		Enabled: true,
		Priorities: []validation.PriorityDef{
			{
				Priority: 1,
				QueryAttributes: []validation.QueryAttribute{
					{
						Regex:         ".*",
						CompiledRegex: regexp.MustCompile(".*"),
						TimeWindow: validation.TimeWindow{
							Start: model.Duration(45 * time.Minute),
							End:   model.Duration(15 * time.Minute),
						},
					},
				},
			},
		},
	}}

	type testCase struct {
		time             time.Time
		start            time.Time
		end              time.Time
		expectedPriority int
	}

	tests := map[string]testCase{
		"should hit instant query between start and end time": {
			time:             now.Add(-30 * time.Minute),
			expectedPriority: 1,
		},
		"should hit instant query equal to start time": {
			time:             now.Add(-45 * time.Minute),
			expectedPriority: 1,
		},
		"should hit instant query equal to end time": {
			time:             now.Add(-15 * time.Minute),
			expectedPriority: 1,
		},
		"should miss instant query outside of end time": {
			expectedPriority: 0,
		},
		"should miss instant query outside of start time": {
			time:             now.Add(-60 * time.Minute),
			expectedPriority: 0,
		},
		"should hit range query between start and end time": {
			start:            now.Add(-40 * time.Minute),
			end:              now.Add(-20 * time.Minute),
			expectedPriority: 1,
		},
		"should hit range query equal to start and end time": {
			start:            now.Add(-45 * time.Minute),
			end:              now.Add(-15 * time.Minute),
			expectedPriority: 1,
		},
		"should miss range query outside of start time": {
			start:            now.Add(-50 * time.Minute),
			end:              now.Add(-15 * time.Minute),
			expectedPriority: 0,
		},
		"should miss range query completely outside of start time": {
			start:            now.Add(-50 * time.Minute),
			end:              now.Add(-45 * time.Minute),
			expectedPriority: 0,
		},
		"should miss range query outside of end time": {
			start:            now.Add(-45 * time.Minute),
			end:              now.Add(-10 * time.Minute),
			expectedPriority: 0,
		},
		"should miss range query completely outside of end time": {
			start:            now.Add(-15 * time.Minute),
			end:              now.Add(-10 * time.Minute),
			expectedPriority: 0,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			var url string
			if !testData.time.IsZero() {
				url = "/query?query=sum(up)&time=" + strconv.FormatInt(testData.time.Unix(), 10)
			} else if !testData.start.IsZero() {
				url = "/query_range?query=sum(up)&start=" + strconv.FormatInt(testData.start.Unix(), 10)
				url += "&end=" + strconv.FormatInt(testData.end.Unix(), 10)
			} else {
				url = "/query?query=sum(up)"
			}
			req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader([]byte{}))
			priority, err := GetPriority(req, "", limits, now, 0)
			assert.NoError(t, err)
			assert.Equal(t, int64(testData.expectedPriority), priority)
		})
	}
}

func Test_GetPriorityShouldNotConsiderStartAndEndTimeIfEmpty(t *testing.T) {
	now := time.Now()
	limits := mockLimits{queryPriority: validation.QueryPriority{
		Enabled: true,
		Priorities: []validation.PriorityDef{
			{
				Priority: 1,
				QueryAttributes: []validation.QueryAttribute{
					{
						Regex: "^sum\\(up\\)$",
					},
				},
			},
		},
	}}

	type testCase struct {
		time  time.Time
		start time.Time
		end   time.Time
	}

	tests := map[string]testCase{
		"should hit instant query with no time": {},
		"should hit instant query with future time": {
			time: now.Add(1000000 * time.Hour),
		},
		"should hit instant query with very old time": {
			time: now.Add(-1000000 * time.Hour),
		},
		"should hit range query with very wide time window": {
			start: now.Add(-1000000 * time.Hour),
			end:   now.Add(1000000 * time.Hour),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			var url string
			if !testData.time.IsZero() {
				url = "/query?query=sum(up)&time=" + strconv.FormatInt(testData.time.Unix(), 10)
			} else if !testData.start.IsZero() {
				url = "/query_range?query=sum(up)&start=" + strconv.FormatInt(testData.start.Unix(), 10)
				url += "&end=" + strconv.FormatInt(testData.end.Unix(), 10)
			} else {
				url = "/query?query=sum(up)"
			}
			req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader([]byte{}))
			priority, err := GetPriority(req, "", limits, now, 0)
			assert.NoError(t, err)
			assert.Equal(t, int64(1), priority)
		})
	}
}
