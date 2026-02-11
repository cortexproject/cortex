// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package function

import (
	"context"
	"sync"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"

	"github.com/prometheus/prometheus/model/labels"
)

type absentOperator struct {
	once     sync.Once
	funcExpr *logicalplan.FunctionCall
	series   []labels.Labels
	next     model.VectorOperator
}

func newAbsentOperator(
	funcExpr *logicalplan.FunctionCall,
	next model.VectorOperator,
	opts *query.Options,
) model.VectorOperator {
	oper := &absentOperator{
		funcExpr: funcExpr,
		next:     next,
	}
	return telemetry.NewOperator(telemetry.NewTelemetry(oper, opts), oper)
}

func (o *absentOperator) String() string {
	return "[absent]"
}

func (o *absentOperator) Explain() (next []model.VectorOperator) {
	return []model.VectorOperator{o.next}
}

func (o *absentOperator) Series(_ context.Context) ([]labels.Labels, error) {
	o.loadSeries()
	return o.series, nil
}

func (o *absentOperator) loadSeries() {
	// we need to put the filtered labels back for absent to compute its series properly
	o.once.Do(func() {
		// https://github.com/prometheus/prometheus/blob/df1b4da348a7c2f8c0b294ffa1f05db5f6641278/promql/functions.go#L1857
		var lm []*labels.Matcher
		switch n := o.funcExpr.Args[0].(type) {
		case *logicalplan.VectorSelector:
			lm = append(n.LabelMatchers, n.Filters...)
		case *logicalplan.MatrixSelector:
			v := n.VectorSelector
			lm = append(v.LabelMatchers, v.Filters...)
		default:
			o.series = []labels.Labels{labels.EmptyLabels()}
			return
		}

		has := make(map[string]bool)
		b := labels.NewBuilder(labels.EmptyLabels())
		for _, l := range lm {
			if l.Name == labels.MetricName {
				continue
			}
			if l.Type == labels.MatchEqual && !has[l.Name] {
				b.Set(l.Name, l.Value)
				has[l.Name] = true
			} else {
				b.Del(l.Name)
			}
		}
		o.series = []labels.Labels{b.Labels()}
	})
}

func (o *absentOperator) Next(ctx context.Context, buf []model.StepVector) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	o.loadSeries()

	n, err := o.next.Next(ctx, buf)
	if err != nil {
		return 0, err
	}

	for i := range n {
		vector := &buf[i]
		isEmpty := len(vector.Samples) == 0 && len(vector.Histograms) == 0
		vector.Reset(vector.T)
		if isEmpty {
			vector.AppendSample(0, 1)
		}
	}
	return n, nil
}
