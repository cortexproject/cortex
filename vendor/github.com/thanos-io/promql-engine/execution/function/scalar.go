// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package function

import (
	"context"
	"math"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/query"

	"github.com/prometheus/prometheus/model/labels"
)

type scalarOperator struct {
	next model.VectorOperator
}

func newScalarOperator(next model.VectorOperator, opts *query.Options) model.VectorOperator {
	oper := &scalarOperator{
		next: next,
	}

	return telemetry.NewOperator(telemetry.NewTelemetry(oper, opts), oper)
}

func (o *scalarOperator) String() string {
	return "[scalar]"
}

func (o *scalarOperator) Explain() (next []model.VectorOperator) {
	return []model.VectorOperator{o.next}
}

func (o *scalarOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	return nil, nil
}

func (o *scalarOperator) Next(ctx context.Context, buf []model.StepVector) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	n, err := o.next.Next(ctx, buf)
	if err != nil {
		return 0, err
	}

	for i := range n {
		vector := &buf[i]
		var val float64
		if len(vector.Samples) == 1 {
			val = vector.Samples[0]
		} else {
			val = math.NaN()
		}
		vector.Reset(vector.T)
		vector.AppendSample(0, val)
	}

	return n, nil
}
