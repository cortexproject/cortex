package ruler

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrefetchCache_FindSuperset(t *testing.T) {
	cache := &prefetchCache{
		entries: []prefetchEntry{
			{
				matchers: []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, "__name__", "http"),
					labels.MustNewMatcher(labels.MatchRegexp, "job", ".*"),
				},
				vector: promql.Vector{
					{Metric: labels.FromStrings("__name__", "http", "job", "api"), T: 1000, F: 1.0},
					{Metric: labels.FromStrings("__name__", "http", "job", "web"), T: 1000, F: 2.0},
				},
			},
		},
	}

	queryMatchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "http"),
		labels.MustNewMatcher(labels.MatchEqual, "job", "api"),
	}

	vec, ok := cache.get(queryMatchers)
	require.True(t, ok)
	assert.Len(t, vec, 1)
	assert.Equal(t, "api", vec[0].Metric.Get("job"))
	assert.Equal(t, 1.0, vec[0].F)
}

func TestIsMatcherSetSuperset(t *testing.T) {
	tests := []struct {
		name  string
		super []*labels.Matcher
		sub   []*labels.Matcher
		want  bool
	}{
		{
			name:  "identical sets",
			super: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "job", "api")},
			sub:   []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "job", "api")},
			want:  true,
		},
		{
			name:  "regex superset of equal",
			super: []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "job", ".*")},
			sub:   []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "job", "api")},
			want:  true,
		},
		{
			name: "super has extra label — more restrictive, not superset",
			super: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "api"),
				labels.MustNewMatcher(labels.MatchEqual, "env", "prod"),
			},
			sub:  []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "job", "api")},
			want: false,
		},
		{
			name:  "sub has extra label — super is broader",
			super: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "job", "api")},
			sub: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "api"),
				labels.MustNewMatcher(labels.MatchEqual, "env", "prod"),
			},
			want: true,
		},
		{
			name:  "different values — not superset",
			super: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "job", "web")},
			sub:   []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "job", "api")},
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isMatcherSetSuperset(tt.super, tt.sub))
		})
	}
}

func TestSelectMergerQueryFunc(t *testing.T) {
	allSeries := promql.Vector{
		{Metric: labels.FromStrings("__name__", "http_requests", "job", "api"), T: 1000, F: 10.0},
		{Metric: labels.FromStrings("__name__", "http_requests", "job", "web"), T: 1000, F: 20.0},
	}

	innerCalled := 0
	inner := func(_ context.Context, qs string, _ time.Time) (promql.Vector, error) {
		innerCalled++
		return allSeries, nil
	}

	qf := selectMergerQueryFunc(inner)

	// Without plan in context — falls through to inner.
	vec, err := qf(context.Background(), `http_requests{job="api"}`, time.Unix(1, 0))
	require.NoError(t, err)
	assert.Equal(t, 1, innerCalled)
	assert.Len(t, vec, 2) // inner returns all

	// With plan in context — lazy prefetch then serve from cache.
	innerCalled = 0
	plan := []mergedSelect{
		{
			metricName: "http_requests",
			mergedMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_requests"),
				labels.MustNewMatcher(labels.MatchRegexp, "job", ".*"),
			},
			prefetchExpr: `http_requests{job=~".*"}`,
		},
	}
	ctx := withSelectMergerPlan(context.Background(), plan)

	// First call triggers prefetch (1 inner call), then serves filtered result.
	vec, err = qf(ctx, `http_requests{job="api"}`, time.Unix(1, 0))
	require.NoError(t, err)
	assert.Equal(t, 1, innerCalled) // prefetch call
	assert.Len(t, vec, 1)
	assert.Equal(t, "api", vec[0].Metric.Get("job"))

	// Second call — served from cache, no additional inner call.
	vec, err = qf(ctx, `http_requests{job="web"}`, time.Unix(1, 0))
	require.NoError(t, err)
	assert.Equal(t, 1, innerCalled) // still 1
	assert.Len(t, vec, 1)
	assert.Equal(t, "web", vec[0].Metric.Get("job"))

	// Query not in cache — falls through to inner.
	vec, err = qf(ctx, `other_metric{job="api"}`, time.Unix(1, 0))
	require.NoError(t, err)
	assert.Equal(t, 2, innerCalled)
}

func TestExecutePrefetch(t *testing.T) {
	queryFunc := func(_ context.Context, _ string, ts time.Time) (promql.Vector, error) {
		return promql.Vector{
			{Metric: labels.FromStrings("__name__", "http", "job", "api"), T: ts.UnixMilli(), F: 5.0},
		}, nil
	}

	plan := []mergedSelect{
		{
			metricName: "http",
			mergedMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "http"),
			},
			prefetchExpr: `http{job=~".*"}`,
		},
	}

	cache := executePrefetch(context.Background(), plan, rules.QueryFunc(queryFunc), time.Unix(1, 0))
	require.Len(t, cache.entries, 1)
	assert.Len(t, cache.entries[0].vector, 1)
}
