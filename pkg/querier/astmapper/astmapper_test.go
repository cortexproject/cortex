package astmapper

import (
	"fmt"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCloneNode(t *testing.T) {
	var testExpr = []struct {
		input    promql.Expr
		expected promql.Expr
	}{
		// simple unmodified case
		{
			&promql.BinaryExpr{promql.ItemADD, &promql.NumberLiteral{1}, &promql.NumberLiteral{1}, nil, false},
			&promql.BinaryExpr{promql.ItemADD, &promql.NumberLiteral{1}, &promql.NumberLiteral{1}, nil, false},
		},
		{
			&promql.AggregateExpr{
				Op:      promql.ItemSum,
				Without: true,
				Expr: &promql.VectorSelector{
					Name: "some_metric",
					LabelMatchers: []*labels.Matcher{
						mustLabelMatcher(labels.MatchEqual, string(model.MetricNameLabel), "some_metric"),
					},
				},
				Grouping: []string{"foo"},
			},
			&promql.AggregateExpr{
				Op:      promql.ItemSum,
				Without: true,
				Expr: &promql.VectorSelector{
					Name: "some_metric",
					LabelMatchers: []*labels.Matcher{
						mustLabelMatcher(labels.MatchEqual, string(model.MetricNameLabel), "some_metric"),
					},
				},
				Grouping: []string{"foo"},
			},
		},
	}

	for i, c := range testExpr {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			res, err := CloneNode(c.input)
			require.NoError(t, err)
			require.Equal(t, c.expected, res)
		})
	}
}

func TestCloneNode_String(t *testing.T) {
	var testExpr = []struct {
		input    string
		expected string
	}{
		{
			`rate(http_requests_total{cluster="us-central1"}[1m])`,
			`rate(http_requests_total{cluster="us-central1"}[1m])`,
		},
		{
			`sum(
sum(rate(http_requests_total{cluster="us-central1"}[1m]))
/
sum(rate(http_requests_total{cluster="ops-tools1"}[1m]))
)`,
			`sum(sum(rate(http_requests_total{cluster="us-central1"}[1m])) / sum(rate(http_requests_total{cluster="ops-tools1"}[1m])))`,
		},
	}

	for i, c := range testExpr {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			expr, err := promql.ParseExpr(c.input)
			require.Nil(t, err)
			res, err := CloneNode(expr)
			require.Nil(t, err)
			require.Equal(t, c.expected, res.String())
		})
	}
}

func mustLabelMatcher(mt labels.MatchType, name, val string) *labels.Matcher {
	m, err := labels.NewMatcher(mt, name, val)
	if err != nil {
		panic(err)
	}
	return m
}
