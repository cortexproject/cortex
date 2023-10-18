package query

import (
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cortexproject/cortex/pkg/util/validation"
)

func Test_IsHighPriority_DefaultValues(t *testing.T) {
	now := time.Now()
	config := []validation.HighPriorityQuery{
		{}, // By default, it should match all queries happened at "now"
	}

	assert.True(t, IsHighPriority(url.Values{
		"query": []string{"count(up)"},
		"time":  []string{strconv.FormatInt(now.UnixMilli(), 10)},
	}, config))
	assert.False(t, IsHighPriority(url.Values{
		"query": []string{"count(up)"},
		"time":  []string{strconv.FormatInt(now.Add(-1*time.Second).UnixMilli(), 10)},
	}, config))
}

func Test_IsHighPriority_ShouldMatchRegex(t *testing.T) {
	now := time.Now()
	config := []validation.HighPriorityQuery{
		{
			Regex: "sum",
		},
	}

	assert.True(t, IsHighPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.UnixMilli(), 10)},
	}, config))
	assert.False(t, IsHighPriority(url.Values{
		"query": []string{"count(up)"},
		"time":  []string{strconv.FormatInt(now.UnixMilli(), 10)},
	}, config))

	config = []validation.HighPriorityQuery{
		{
			Regex: "up",
		},
	}

	assert.True(t, IsHighPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.UnixMilli(), 10)},
	}, config))
	assert.True(t, IsHighPriority(url.Values{
		"query": []string{"count(up)"},
		"time":  []string{strconv.FormatInt(now.UnixMilli(), 10)},
	}, config))

	config = []validation.HighPriorityQuery{
		{
			Regex: "sum",
		},
		{
			Regex: "c(.+)t",
		},
	}

	assert.True(t, IsHighPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.UnixMilli(), 10)},
	}, config))
	assert.True(t, IsHighPriority(url.Values{
		"query": []string{"count(up)"},
		"time":  []string{strconv.FormatInt(now.UnixMilli(), 10)},
	}, config))

	config = []validation.HighPriorityQuery{
		{
			Regex: "doesnotexist",
		},
		{
			Regex: "^sum$",
		},
	}

	assert.False(t, IsHighPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.UnixMilli(), 10)},
	}, config))
	assert.False(t, IsHighPriority(url.Values{
		"query": []string{"count(up)"},
		"time":  []string{strconv.FormatInt(now.UnixMilli(), 10)},
	}, config))

	config = []validation.HighPriorityQuery{
		{
			Regex: ".*",
		},
	}

	assert.True(t, IsHighPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.UnixMilli(), 10)},
	}, config))
	assert.True(t, IsHighPriority(url.Values{
		"query": []string{"count(up)"},
		"time":  []string{strconv.FormatInt(now.UnixMilli(), 10)},
	}, config))
}

func Test_IsHighPriority_ShouldBeBetweenStartAndEndTime(t *testing.T) {
	now := time.Now()
	config := []validation.HighPriorityQuery{
		{
			StartTime: 1 * time.Hour,
			EndTime:   30 * time.Minute,
		},
	}

	assert.False(t, IsHighPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Add(-2*time.Hour).UnixMilli(), 10)},
	}, config))
	assert.True(t, IsHighPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Add(-1*time.Hour).UnixMilli(), 10)},
	}, config))
	assert.True(t, IsHighPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Add(-30*time.Minute).UnixMilli(), 10)},
	}, config))
	assert.False(t, IsHighPriority(url.Values{
		"query": []string{"sum(up)"},
		"time":  []string{strconv.FormatInt(now.Add(-1*time.Minute).UnixMilli(), 10)},
	}, config))
	assert.False(t, IsHighPriority(url.Values{
		"query": []string{"sum(up)"},
		"start": []string{strconv.FormatInt(now.Add(-2*time.Hour).UnixMilli(), 10)},
		"end":   []string{strconv.FormatInt(now.Add(-30*time.Minute).UnixMilli(), 10)},
	}, config))
	assert.True(t, IsHighPriority(url.Values{
		"query": []string{"sum(up)"},
		"start": []string{strconv.FormatInt(now.Add(-1*time.Hour).UnixMilli(), 10)},
		"end":   []string{strconv.FormatInt(now.Add(-30*time.Minute).UnixMilli(), 10)},
	}, config))
	assert.False(t, IsHighPriority(url.Values{
		"query": []string{"sum(up)"},
		"start": []string{strconv.FormatInt(now.Add(-1*time.Hour).UnixMilli(), 10)},
		"end":   []string{strconv.FormatInt(now.UnixMilli(), 10)},
	}, config))
}
