// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package unary

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"gonum.org/v1/gonum/floats"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/query"
)

type unaryNegation struct {
	next model.VectorOperator
	once sync.Once

	series []labels.Labels
	model.OperatorTelemetry
}

func NewUnaryNegation(next model.VectorOperator, opts *query.Options) (model.VectorOperator, error) {
	u := &unaryNegation{
		next: next,
	}
	u.OperatorTelemetry = model.NewTelemetry(u, opts)

	return u, nil
}

func (u *unaryNegation) Explain() (next []model.VectorOperator) {
	return []model.VectorOperator{u.next}
}

func (u *unaryNegation) String() string {
	return "[unaryNegation]"
}

func (u *unaryNegation) Series(ctx context.Context) ([]labels.Labels, error) {
	start := time.Now()
	defer func() { u.AddExecutionTimeTaken(time.Since(start)) }()

	if err := u.loadSeries(ctx); err != nil {
		return nil, err
	}
	return u.series, nil
}

func (u *unaryNegation) loadSeries(ctx context.Context) error {
	var err error
	u.once.Do(func() {
		var series []labels.Labels
		series, err = u.next.Series(ctx)
		if err != nil {
			return
		}
		u.series = make([]labels.Labels, len(series))
		for i := range series {
			lbls := labels.NewBuilder(series[i]).Del(labels.MetricName).Labels()
			u.series[i] = lbls
		}
	})
	return err
}

func (u *unaryNegation) GetPool() *model.VectorPool {
	return u.next.GetPool()
}

func (u *unaryNegation) Next(ctx context.Context) ([]model.StepVector, error) {
	start := time.Now()
	defer func() { u.AddExecutionTimeTaken(time.Since(start)) }()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	in, err := u.next.Next(ctx)
	if err != nil {
		return nil, err
	}
	if in == nil {
		return nil, nil
	}
	for i := range in {
		floats.Scale(-1, in[i].Samples)
	}
	return in, nil
}
