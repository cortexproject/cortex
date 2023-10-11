// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package function

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-io/promql-engine/execution/model"
)

type absentOperator struct {
	once     sync.Once
	funcExpr *parser.Call
	series   []labels.Labels
	pool     *model.VectorPool
	next     model.VectorOperator
	model.OperatorTelemetry
}

func (o *absentOperator) Analyze() (model.OperatorTelemetry, []model.ObservableVectorOperator) {
	o.SetName("[*absentOperator]")
	next := make([]model.ObservableVectorOperator, 0, 1)
	if obsnext, ok := o.next.(model.ObservableVectorOperator); ok {
		next = append(next, obsnext)
	}
	return o, next
}

func (o *absentOperator) Explain() (me string, next []model.VectorOperator) {
	return "[*absentOperator]", []model.VectorOperator{}
}

func (o *absentOperator) Series(_ context.Context) ([]labels.Labels, error) {
	o.loadSeries()
	return o.series, nil
}

func (o *absentOperator) loadSeries() {
	o.once.Do(func() {
		o.pool.SetStepSize(1)

		// https://github.com/prometheus/prometheus/blob/main/promql/functions.go#L1385
		var lm []*labels.Matcher
		switch n := o.funcExpr.Args[0].(type) {
		case *parser.VectorSelector:
			lm = n.LabelMatchers
		case *parser.MatrixSelector:
			lm = n.VectorSelector.(*parser.VectorSelector).LabelMatchers
		default:
			o.series = []labels.Labels{labels.EmptyLabels()}
			return
		}

		has := make(map[string]bool)
		lmap := make(map[string]string)
		for _, l := range lm {
			if l.Name == labels.MetricName {
				continue
			}
			if l.Type == labels.MatchEqual && !has[l.Name] {
				lmap[l.Name] = l.Value
				has[l.Name] = true
			} else {
				delete(lmap, l.Name)
			}
		}
		o.series = []labels.Labels{labels.FromMap(lmap)}
	})
}

func (o *absentOperator) GetPool() *model.VectorPool {
	return o.pool
}

func (o *absentOperator) Next(ctx context.Context) ([]model.StepVector, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	o.loadSeries()
	start := time.Now()

	vectors, err := o.next.Next(ctx)
	if err != nil {
		return nil, err
	}
	if len(vectors) == 0 {
		return nil, nil
	}

	result := o.GetPool().GetVectorBatch()
	for i := range vectors {
		sv := o.GetPool().GetStepVector(vectors[i].T)
		if len(vectors[i].Samples) == 0 && len(vectors[i].Histograms) == 0 {
			sv.AppendSample(o.GetPool(), 0, 1)
		}
		result = append(result, sv)
		o.next.GetPool().PutStepVector(vectors[i])
	}
	o.next.GetPool().PutVectors(vectors)
	o.AddExecutionTimeTaken(time.Since(start))
	return result, nil
}
