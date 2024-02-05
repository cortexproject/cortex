package tripperware

import (
	"regexp"
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
		query                string
		queryPriorityEnabled bool
	}

	tests := map[string]testCase{
		"should miss if query priority not enabled": {
			query: "up",
		},
		"should miss if query string empty": {
			query:                "",
			queryPriorityEnabled: true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			limits.queryPriority.Enabled = testData.queryPriorityEnabled
			priority := GetPriority(testData.query, 0, 0, now, limits.queryPriority)
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
			priority := GetPriority(testData.query, 0, 0, now, limits.queryPriority)
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
		start            time.Time
		end              time.Time
		expectedPriority int
	}

	tests := map[string]testCase{
		"should hit between start and end time": {
			start:            now.Add(-40 * time.Minute),
			end:              now.Add(-20 * time.Minute),
			expectedPriority: 1,
		},
		"should hit equal to start and end time": {
			start:            now.Add(-45 * time.Minute),
			end:              now.Add(-15 * time.Minute),
			expectedPriority: 1,
		},
		"should miss outside of start time": {
			start:            now.Add(-50 * time.Minute),
			end:              now.Add(-15 * time.Minute),
			expectedPriority: 0,
		},
		"should miss completely outside of start time": {
			start:            now.Add(-50 * time.Minute),
			end:              now.Add(-45 * time.Minute),
			expectedPriority: 0,
		},
		"should miss outside of end time": {
			start:            now.Add(-45 * time.Minute),
			end:              now.Add(-10 * time.Minute),
			expectedPriority: 0,
		},
		"should miss completely outside of end time": {
			start:            now.Add(-15 * time.Minute),
			end:              now.Add(-10 * time.Minute),
			expectedPriority: 0,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			priority := GetPriority("sum(up)", testData.start.UnixMilli(), testData.end.UnixMilli(), now, limits.queryPriority)
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
		start time.Time
		end   time.Time
	}

	tests := map[string]testCase{
		"should hit with future time": {
			start: now,
			end:   now.Add(1000000 * time.Hour),
		},
		"should hit with very old time": {
			start: now.Add(-1000000 * time.Hour),
			end:   now,
		},
		"should hit with very wide time window": {
			start: now.Add(-1000000 * time.Hour),
			end:   now.Add(1000000 * time.Hour),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			priority := GetPriority("sum(up)", testData.start.Unix(), testData.end.Unix(), now, limits.queryPriority)
			assert.Equal(t, int64(1), priority)
		})
	}
}
