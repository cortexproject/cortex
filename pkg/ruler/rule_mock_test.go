package ruler

import (
	"context"
	"net/url"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/rules"
)

type mockRule struct {
	name                string
	health              rules.RuleHealth
	lastError           error
	evaluationTimestamp time.Time
	evaluationDuration  time.Duration
}

func (r *mockRule) Name() string {
	return r.name
}

func (r *mockRule) Labels() labels.Labels {
	return labels.EmptyLabels()
}

func (r *mockRule) Eval(context.Context, time.Time, rules.QueryFunc, *url.URL, int) (promql.Vector, error) {
	return promql.Vector{}, nil
}

func (r *mockRule) String() string {
	return r.name
}

func (r *mockRule) Query() parser.Expr {
	return &parser.StringLiteral{}
}

func (r *mockRule) SetLastError(e error) {
	r.lastError = e
}

func (r *mockRule) LastError() error {
	return r.lastError
}

func (r *mockRule) SetHealth(h rules.RuleHealth) {
	r.health = h
}

func (r *mockRule) Health() rules.RuleHealth {
	return r.health
}

func (r *mockRule) SetEvaluationDuration(d time.Duration) {
	r.evaluationDuration = d
}

func (r *mockRule) GetEvaluationDuration() time.Duration {
	return r.evaluationDuration
}

func (r *mockRule) SetEvaluationTimestamp(t time.Time) {
	r.evaluationTimestamp = t
}

func (r *mockRule) GetEvaluationTimestamp() time.Time {
	return r.evaluationTimestamp
}
