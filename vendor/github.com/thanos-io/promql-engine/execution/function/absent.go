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
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
)

type absentOperator struct {
	model.OperatorTelemetry

	once     sync.Once
	funcExpr *parser.Call
	series   []labels.Labels
	pool     *model.VectorPool
	next     model.VectorOperator
}

func newAbsentOperator(
	funcExpr *parser.Call,
	pool *model.VectorPool,
	next model.VectorOperator,
	opts *query.Options,
) *absentOperator {
	return &absentOperator{
		OperatorTelemetry: model.NewTelemetry(absentOperatorName, opts.EnableAnalysis),
		funcExpr:          funcExpr,
		pool:              pool,
		next:              next,
	}
}

func (o *absentOperator) Explain() (me string, next []model.VectorOperator) {
	return absentOperatorName, []model.VectorOperator{o.next}
}

func (o *absentOperator) Series(_ context.Context) ([]labels.Labels, error) {
	start := time.Now()
	defer func() { o.AddExecutionTimeTaken(time.Since(start)) }()

	o.loadSeries()
	return o.series, nil
}

func (o *absentOperator) loadSeries() {
	// we need to put the filtered labels back for absent to compute its series properly
	o.once.Do(func() {
		o.pool.SetStepSize(1)

		// https://github.com/prometheus/prometheus/blob/main/promql/functions.go#L1385
		var lm []*labels.Matcher
		switch n := o.funcExpr.Args[0].(type) {
		case *logicalplan.VectorSelector:
			lm = append(n.LabelMatchers, n.Filters...)
		case *logicalplan.MatrixSelector:
			v := n.VectorSelector.(*logicalplan.VectorSelector)
			lm = append(v.LabelMatchers, v.Filters...)
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
	start := time.Now()
	defer func() { o.AddExecutionTimeTaken(time.Since(start)) }()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	o.loadSeries()

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
	return result, nil
}
