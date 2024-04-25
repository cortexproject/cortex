// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package function

import (
	"context"
	"math"
	"time"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/query"
)

type scalarOperator struct {
	pool *model.VectorPool
	next model.VectorOperator
	model.OperatorTelemetry
}

func newScalarOperator(pool *model.VectorPool, next model.VectorOperator, opts *query.Options) *scalarOperator {
	oper := &scalarOperator{
		pool: pool,
		next: next,
	}

	oper.OperatorTelemetry = model.NewTelemetry(oper, opts.EnableAnalysis)
	return oper
}

func (o *scalarOperator) String() string {
	return "[scalar]"
}

func (o *scalarOperator) Explain() (next []model.VectorOperator) {
	return []model.VectorOperator{o.next}
}

func (o *scalarOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	start := time.Now()
	defer func() { o.AddExecutionTimeTaken(time.Since(start)) }()

	return nil, nil
}

func (o *scalarOperator) GetPool() *model.VectorPool {
	return o.pool
}

func (o *scalarOperator) Next(ctx context.Context) ([]model.StepVector, error) {
	start := time.Now()
	defer func() { o.AddExecutionTimeTaken(time.Since(start)) }()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	in, err := o.next.Next(ctx)
	if err != nil {
		return nil, err
	}
	if len(in) == 0 {
		return nil, nil
	}

	result := o.GetPool().GetVectorBatch()
	for _, vector := range in {
		sv := o.GetPool().GetStepVector(vector.T)
		if len(vector.Samples) != 1 {
			sv.AppendSample(o.GetPool(), 0, math.NaN())
		} else {
			sv.AppendSample(o.GetPool(), 0, vector.Samples[0])
		}
		result = append(result, sv)
		o.next.GetPool().PutStepVector(vector)
	}
	o.next.GetPool().PutVectors(in)

	return result, nil
}
