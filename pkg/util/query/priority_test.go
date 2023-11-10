package query

import (
	"net/url"
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
					Regex:     ".*",
					StartTime: 2 * time.Hour,
					EndTime:   0 * time.Hour,
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
	}, now, &queryPriority, true))

	queryPriority.Enabled = true
	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{""},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, &queryPriority, true))
}

func Test_GetPriorityShouldConsiderRegex(t *testing.T) {
	now := time.Now()
	priorities := []validation.PriorityDef{
		{
			Priority: 1,
			QueryAttributes: []validation.QueryAttribute{
				{
					Regex:     "sum",
					StartTime: 2 * time.Hour,
					EndTime:   0 * time.Hour,
				},
			},
		},
	}
	queryPriority := validation.QueryPriority{
		Enabled:    true,
		Priorities: priorities,
	}

	assert.Nil(t, queryPriority.Priorities[0].QueryAttributes[0].CompiledRegex)
	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, &queryPriority, true))
	assert.NotNil(t, queryPriority.Priorities[0].QueryAttributes[0].CompiledRegex)
	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{"count(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, &queryPriority, false))

	queryPriority.Priorities[0].QueryAttributes[0].Regex = "(^sum$|c(.+)t)"

	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, &queryPriority, true))
	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"count(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, &queryPriority, false))

	queryPriority.Priorities[0].QueryAttributes[0].Regex = ".*"

	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, &queryPriority, true))
	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"count(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, &queryPriority, false))

	queryPriority.Priorities[0].QueryAttributes[0].Regex = ".+"

	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, &queryPriority, true))
	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"count(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, &queryPriority, false))

	queryPriority.Priorities[0].QueryAttributes[0].Regex = ""

	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, &queryPriority, true))
	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"count(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, &queryPriority, false))
}

func Test_GetPriorityShouldNotRecompileRegexIfQueryPriorityChangedIsTrue(t *testing.T) {
	now := time.Now()
	priorities := []validation.PriorityDef{
		{
			Priority: 1,
			QueryAttributes: []validation.QueryAttribute{
				{
					Regex:     "sum",
					StartTime: 2 * time.Hour,
					EndTime:   0 * time.Hour,
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
	}, now, &queryPriority, true))

	queryPriority.Priorities[0].QueryAttributes[0].Regex = "count"

	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{"count(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, &queryPriority, false))
	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"count(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, &queryPriority, true))
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
	}, now, &queryPriority, true))
	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Add(-30*time.Minute).Unix(), 10)},
	}, now, &queryPriority, false))
	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Add(-60*time.Minute).Unix(), 10)},
	}, now, &queryPriority, false))
	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Add(-45*time.Minute).Unix(), 10)},
	}, now, &queryPriority, false))
	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Add(-15*time.Minute).Unix(), 10)},
	}, now, &queryPriority, false))

	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"start": []string{strconv.FormatInt(now.Add(-45*time.Minute).Unix(), 10)},
		"end":   []string{strconv.FormatInt(now.Add(-15*time.Minute).Unix(), 10)},
	}, now, &queryPriority, false))
	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"start": []string{strconv.FormatInt(now.Add(-50*time.Minute).Unix(), 10)},
		"end":   []string{strconv.FormatInt(now.Add(-15*time.Minute).Unix(), 10)},
	}, now, &queryPriority, false))
	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"start": []string{strconv.FormatInt(now.Add(-45*time.Minute).Unix(), 10)},
		"end":   []string{strconv.FormatInt(now.Add(-10*time.Minute).Unix(), 10)},
	}, now, &queryPriority, false))
	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"start": []string{strconv.FormatInt(now.Add(-60*time.Minute).Unix(), 10)},
		"end":   []string{strconv.FormatInt(now.Add(-45*time.Minute).Unix(), 10)},
	}, now, &queryPriority, false))
	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"start": []string{strconv.FormatInt(now.Add(-15*time.Minute).Unix(), 10)},
		"end":   []string{strconv.FormatInt(now.Add(-1*time.Minute).Unix(), 10)},
	}, now, &queryPriority, false))
}
