package query

import (
	"net/url"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cortexproject/cortex/pkg/util/validation"
)

func Test_GetPriorityShouldReturnDefaultPriorityIfNotEnabledOrEmptyQueryString(t *testing.T) {
	now := time.Now()
	priorities := []validation.PriorityDef{
		{
			Priority: 1,
			QueryAttributes: []validation.QueryAttribute{
				{
					Regex:         ".*",
					CompiledRegex: regexp.MustCompile(".*"),
					StartTime:     2 * time.Hour,
					EndTime:       0 * time.Hour,
				},
			},
		},
	}
	queryPriority := validation.QueryPriority{
		Priorities: priorities,
	}

	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, queryPriority))

	queryPriority.Enabled = true
	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{""},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, queryPriority))
}

func Test_GetPriorityShouldConsiderRegex(t *testing.T) {
	now := time.Now()
	priorities := []validation.PriorityDef{
		{
			Priority: 1,
			QueryAttributes: []validation.QueryAttribute{
				{
					Regex:         "sum",
					CompiledRegex: regexp.MustCompile("sum"),
					StartTime:     2 * time.Hour,
					EndTime:       0 * time.Hour,
				},
			},
		},
	}
	queryPriority := validation.QueryPriority{
		Enabled:    true,
		Priorities: priorities,
	}

	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, queryPriority))
	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{"count(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, queryPriority))

	queryPriority.Priorities[0].QueryAttributes[0].Regex = "(^sum$|c(.+)t)"
	queryPriority.Priorities[0].QueryAttributes[0].CompiledRegex = regexp.MustCompile("(^sum$|c(.+)t)")

	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, queryPriority))
	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"count(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, queryPriority))

	queryPriority.Priorities[0].QueryAttributes[0].Regex = ".*"
	queryPriority.Priorities[0].QueryAttributes[0].CompiledRegex = regexp.MustCompile(".*")

	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, queryPriority))
	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"count(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, queryPriority))

	queryPriority.Priorities[0].QueryAttributes[0].Regex = ".+"
	queryPriority.Priorities[0].QueryAttributes[0].CompiledRegex = regexp.MustCompile(".+")

	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, queryPriority))
	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"count(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, queryPriority))

	queryPriority.Priorities[0].QueryAttributes[0].Regex = ""
	queryPriority.Priorities[0].QueryAttributes[0].CompiledRegex = regexp.MustCompile("")

	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, queryPriority))
	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"count(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, queryPriority))
}

func Test_GetPriorityShouldConsiderStartAndEndTime(t *testing.T) {
	now := time.Now()
	priorities := []validation.PriorityDef{
		{
			Priority: 1,
			QueryAttributes: []validation.QueryAttribute{
				{
					StartTime: 45 * time.Minute,
					EndTime:   15 * time.Minute,
				},
			},
		},
	}
	queryPriority := validation.QueryPriority{
		Enabled:    true,
		Priorities: priorities,
	}

	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, queryPriority))
	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Add(-30*time.Minute).Unix(), 10)},
	}, now, queryPriority))
	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Add(-60*time.Minute).Unix(), 10)},
	}, now, queryPriority))
	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Add(-45*time.Minute).Unix(), 10)},
	}, now, queryPriority))
	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Add(-15*time.Minute).Unix(), 10)},
	}, now, queryPriority))

	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"start": []string{strconv.FormatInt(now.Add(-45*time.Minute).Unix(), 10)},
		"end":   []string{strconv.FormatInt(now.Add(-15*time.Minute).Unix(), 10)},
	}, now, queryPriority))
	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"start": []string{strconv.FormatInt(now.Add(-50*time.Minute).Unix(), 10)},
		"end":   []string{strconv.FormatInt(now.Add(-15*time.Minute).Unix(), 10)},
	}, now, queryPriority))
	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"start": []string{strconv.FormatInt(now.Add(-45*time.Minute).Unix(), 10)},
		"end":   []string{strconv.FormatInt(now.Add(-10*time.Minute).Unix(), 10)},
	}, now, queryPriority))
	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"start": []string{strconv.FormatInt(now.Add(-60*time.Minute).Unix(), 10)},
		"end":   []string{strconv.FormatInt(now.Add(-45*time.Minute).Unix(), 10)},
	}, now, queryPriority))
	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"start": []string{strconv.FormatInt(now.Add(-15*time.Minute).Unix(), 10)},
		"end":   []string{strconv.FormatInt(now.Add(-1*time.Minute).Unix(), 10)},
	}, now, queryPriority))
}
