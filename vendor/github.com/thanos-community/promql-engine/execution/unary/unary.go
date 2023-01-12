// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package unary

import (
	"context"
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"gonum.org/v1/gonum/floats"

	"github.com/thanos-community/promql-engine/execution/model"
	"github.com/thanos-community/promql-engine/worker"
)

type unaryNegation struct {
	next model.VectorOperator
	once sync.Once

	series []labels.Labels

	workers worker.Group
}

func (u *unaryNegation) Explain() (me string, next []model.VectorOperator) {
	return "[*unaryNegation]", []model.VectorOperator{u.next}
}

func NewUnaryNegation(
	next model.VectorOperator,
	stepsBatch int,
) (model.VectorOperator, error) {
	u := &unaryNegation{
		next: next,
	}

	u.workers = worker.NewGroup(stepsBatch, u.workerTask)
	return u, nil
}

func (u *unaryNegation) Series(ctx context.Context) ([]labels.Labels, error) {
	var err error
	u.once.Do(func() { err = u.loadSeries(ctx) })
	if err != nil {
		return nil, err
	}
	return u.series, nil
}

func (u *unaryNegation) loadSeries(ctx context.Context) error {
	vectorSeries, err := u.next.Series(ctx)
	if err != nil {
		return err
	}
	u.series = make([]labels.Labels, len(vectorSeries))
	for i := range vectorSeries {
		lbls := labels.NewBuilder(vectorSeries[i]).Del(labels.MetricName).Labels(nil)
		u.series[i] = lbls
	}

	u.workers.Start(ctx)
	return nil
}

func (u *unaryNegation) GetPool() *model.VectorPool {
	return u.next.GetPool()
}

func (u *unaryNegation) Next(ctx context.Context) ([]model.StepVector, error) {
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
	for i, vector := range in {
		if err := u.workers[i].Send(0, vector); err != nil {
			return nil, err
		}
	}

	for i := range in {
		// Make sure worker finishes the job.
		// Since it is in-place so no need another buffer.
		if _, err := u.workers[i].GetOutput(); err != nil {
			return nil, err
		}
	}

	return in, nil
}

func (u *unaryNegation) workerTask(_ int, _ float64, vector model.StepVector) model.StepVector {
	floats.Scale(-1, vector.Samples)
	return vector
}
