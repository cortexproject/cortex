package query

import (
	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/stretchr/testify/assert"
	"net/url"
	"strconv"
	"testing"
	"time"
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

	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, validation.QueryPriority{
		Priorities: priorities,
	}))

	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{""},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, validation.QueryPriority{
		Enabled:    true,
		Priorities: priorities,
	}))
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
	limits, _ := validation.NewOverrides(validation.Limits{
		QueryPriority: validation.QueryPriority{
			Enabled:    true,
			Priorities: priorities,
		},
	}, nil)

	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, limits.QueryPriority("")))
	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{"count(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, limits.QueryPriority("")))

	priorities[0].QueryAttributes[0].Regex = "(^sum$|c(.+)t)"
	limits, _ = validation.NewOverrides(validation.Limits{
		QueryPriority: validation.QueryPriority{
			Enabled:    true,
			Priorities: priorities,
		},
	}, nil)

	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, limits.QueryPriority("")))
	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"count(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, limits.QueryPriority("")))

	priorities[0].QueryAttributes[0].Regex = ".*"
	limits, _ = validation.NewOverrides(validation.Limits{
		QueryPriority: validation.QueryPriority{
			Enabled:    true,
			Priorities: priorities,
		},
	}, nil)

	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, limits.QueryPriority("")))
	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"count(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, limits.QueryPriority("")))

	priorities[0].QueryAttributes[0].Regex = ""
	limits, _ = validation.NewOverrides(validation.Limits{
		QueryPriority: validation.QueryPriority{
			Enabled:    true,
			Priorities: priorities,
		},
	}, nil)

	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, limits.QueryPriority("")))
	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"count(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, limits.QueryPriority("")))
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
	limits, _ := validation.NewOverrides(validation.Limits{
		QueryPriority: validation.QueryPriority{
			Enabled:    true,
			Priorities: priorities,
		},
	}, nil)

	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Unix(), 10)},
	}, now, limits.QueryPriority("")))
	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Add(-30*time.Minute).Unix(), 10)},
	}, now, limits.QueryPriority("")))
	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Add(-60*time.Minute).Unix(), 10)},
	}, now, limits.QueryPriority("")))
	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Add(-45*time.Minute).Unix(), 10)},
	}, now, limits.QueryPriority("")))
	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Add(-15*time.Minute).Unix(), 10)},
	}, now, limits.QueryPriority("")))

	assert.Equal(t, int64(1), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"start": []string{strconv.FormatInt(now.Add(-45*time.Minute).Unix(), 10)},
		"end":   []string{strconv.FormatInt(now.Add(-15*time.Minute).Unix(), 10)},
	}, now, limits.QueryPriority("")))
	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"start": []string{strconv.FormatInt(now.Add(-50*time.Minute).Unix(), 10)},
		"end":   []string{strconv.FormatInt(now.Add(-15*time.Minute).Unix(), 10)},
	}, now, limits.QueryPriority("")))
	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"start": []string{strconv.FormatInt(now.Add(-45*time.Minute).Unix(), 10)},
		"end":   []string{strconv.FormatInt(now.Add(-10*time.Minute).Unix(), 10)},
	}, now, limits.QueryPriority("")))
	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"start": []string{strconv.FormatInt(now.Add(-60*time.Minute).Unix(), 10)},
		"end":   []string{strconv.FormatInt(now.Add(-45*time.Minute).Unix(), 10)},
	}, now, limits.QueryPriority("")))
	assert.Equal(t, int64(0), GetPriority(url.Values{
		"query": []string{"sum(up)"},
		"start": []string{strconv.FormatInt(now.Add(-15*time.Minute).Unix(), 10)},
		"end":   []string{strconv.FormatInt(now.Add(-1*time.Minute).Unix(), 10)},
	}, now, limits.QueryPriority("")))
}
