package astmapper

import (
	"fmt"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
)

func TestCloneNode(t *testing.T) {
	var testExpr = []struct {
		input    promql.Expr
		expected promql.Expr
	}{
		// simple unmodified case
		{
			&promql.BinaryExpr{
				Op:  promql.ItemADD,
				LHS: &promql.NumberLiteral{Val: 1},
				RHS: &promql.NumberLiteral{Val: 1},
			},
			&promql.BinaryExpr{
				Op:  promql.ItemADD,
				LHS: &promql.NumberLiteral{Val: 1},
				RHS: &promql.NumberLiteral{Val: 1},
			},
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
			input:    `rate(http_requests_total{cluster="us-central1"}[1m])`,
			expected: `rate(http_requests_total{cluster="us-central1"}[1m])`,
		},
		{
			input: `sum(
sum(rate(http_requests_total{cluster="us-central1"}[1m]))
/
sum(rate(http_requests_total{cluster="ops-tools1"}[1m]))
)`,
			expected: `sum(sum(rate(http_requests_total{cluster="us-central1"}[1m])) / sum(rate(http_requests_total{cluster="ops-tools1"}[1m])))`,
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
