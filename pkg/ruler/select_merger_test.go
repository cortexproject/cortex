package ruler

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/rules"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeRule struct {
	rules.Rule
	expr parser.Expr
}

func (f *fakeRule) Query() parser.Expr { return f.expr }

func mustParseRule(t *testing.T, expr string) rules.Rule {
	t.Helper()
	e, err := parser.ParseExpr(expr)
	require.NoError(t, err)
	return &fakeRule{expr: e}
}

func TestSelectMerger_Plan_GroupsByMetric(t *testing.T) {
	rls := []rules.Rule{
		// http_requests_total: 3 rules with same structure, deploy_color varies, one has =~".*" superset
		mustParseRule(t, `sum(http_requests_total{job="api",deploy_color="blue"})`),
		mustParseRule(t, `sum(http_requests_total{job="api",deploy_color="green"})`),
		mustParseRule(t, `sum(http_requests_total{job="api",deploy_color=~".*"})`),
		// cpu_usage: 3 rules with same structure, host varies, one has =~".*" superset
		mustParseRule(t, `avg(cpu_usage{host="a"})`),
		mustParseRule(t, `avg(cpu_usage{host="b"})`),
		mustParseRule(t, `avg(cpu_usage{host=~".*"})`),
	}

	result := planMergedSelects(rls, 2)

	// Should produce 2 merged selects: one for http_requests_total, one for cpu_usage
	assert.Len(t, result, 2)

	metricNames := map[string]bool{}
	for _, ms := range result {
		metricNames[ms.metricName] = true
	}
	assert.True(t, metricNames["http_requests_total"])
	assert.True(t, metricNames["cpu_usage"])
}

func TestSelectMerger_SupersetDetection(t *testing.T) {
	tests := []struct {
		name     string
		a, b     *labels.Matcher
		aCoversB bool
	}{
		{
			name:     "regex .* covers equality",
			a:        labels.MustNewMatcher(labels.MatchRegexp, "job", ".*"),
			b:        labels.MustNewMatcher(labels.MatchEqual, "job", "blue"),
			aCoversB: true,
		},
		{
			name:     "regex .+ covers non-empty equality",
			a:        labels.MustNewMatcher(labels.MatchRegexp, "job", ".+"),
			b:        labels.MustNewMatcher(labels.MatchEqual, "job", "blue"),
			aCoversB: true,
		},
		{
			name:     "regex .+ does not cover empty",
			a:        labels.MustNewMatcher(labels.MatchRegexp, "job", ".+"),
			b:        labels.MustNewMatcher(labels.MatchEqual, "job", ""),
			aCoversB: false,
		},
		{
			name:     "same equality covers itself",
			a:        labels.MustNewMatcher(labels.MatchEqual, "job", "api"),
			b:        labels.MustNewMatcher(labels.MatchEqual, "job", "api"),
			aCoversB: true,
		},
		{
			name:     "different equality does not cover",
			a:        labels.MustNewMatcher(labels.MatchEqual, "job", "api"),
			b:        labels.MustNewMatcher(labels.MatchEqual, "job", "web"),
			aCoversB: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.aCoversB, isMatcherSuperset(tc.a, tc.b))
		})
	}
}

func TestSelectMerger_MinRulesThreshold(t *testing.T) {
	rls := []rules.Rule{
		mustParseRule(t, `sum(http_requests_total{job="api",color="blue"})`),
		mustParseRule(t, `sum(http_requests_total{job="api",color="green"})`),
		mustParseRule(t, `sum(http_requests_total{job="api",color=~".*"})`),
		mustParseRule(t, `avg(cpu_usage{host="a"})`),
	}

	// minRules=2: http_requests_total has 3 rules with superset, cpu_usage has only 1
	result := planMergedSelects(rls, 2)
	require.Len(t, result, 1)
	assert.Equal(t, "http_requests_total", result[0].metricName)

	// minRules=4: nothing qualifies
	result = planMergedSelects(rls, 4)
	assert.Len(t, result, 0)
}

func TestSelectMerger_DifferentExprStructures_NotMerged(t *testing.T) {
	// Same metric but different expression structures should NOT be merged.
	rls := []rules.Rule{
		mustParseRule(t, `rate(http_requests_total{job="api",color="blue"}[5m])`),
		mustParseRule(t, `rate(http_requests_total{job="api",color=~".*"}[5m])`),
		mustParseRule(t, `sum(http_requests_total{job="api",color="red"})`),
	}

	result := planMergedSelects(rls, 2)
	// Only the rate() pair should merge (2 rules with same structure).
	// The sum() rule has a different structure.
	require.Len(t, result, 1)
	assert.Equal(t, "http_requests_total", result[0].metricName)
	assert.Contains(t, result[0].prefetchExpr, "rate")
}

func TestSelectMerger_NoSuperset_NotMerged(t *testing.T) {
	// When no rule has a superset matcher, merging is skipped.
	rls := []rules.Rule{
		mustParseRule(t, `sum(http_requests_total{job="api"})`),
		mustParseRule(t, `sum(http_requests_total{job="web"})`),
	}

	result := planMergedSelects(rls, 2)
	// job="api" and job="web" have no superset, so no prefetchExpr can be found.
	assert.Len(t, result, 0)
}
