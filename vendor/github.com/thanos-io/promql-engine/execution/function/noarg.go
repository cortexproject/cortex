// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package function

import (
	"context"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/logicalplan"

	"github.com/prometheus/prometheus/model/labels"
)

type noArgFunctionOperator struct {
	mint        int64
	maxt        int64
	step        int64
	currentStep int64
	stepsBatch  int
	funcExpr    *logicalplan.FunctionCall
	call        noArgFunctionCall
	series      []labels.Labels
	sampleIDs   []uint64
}

func (o *noArgFunctionOperator) Explain() (next []model.VectorOperator) {
	return nil
}

func (o *noArgFunctionOperator) String() string {
	return "[noArgFunction]"
}

func (o *noArgFunctionOperator) Series(_ context.Context) ([]labels.Labels, error) {
	return o.series, nil
}

func (o *noArgFunctionOperator) Next(ctx context.Context, buf []model.StepVector) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	if o.currentStep > o.maxt {
		return 0, nil
	}

	n := 0
	maxSteps := min(o.stepsBatch, len(buf))

	for i := 0; i < maxSteps && o.currentStep <= o.maxt; i++ {
		buf[n].Reset(o.currentStep)
		buf[n].AppendSample(o.sampleIDs[0], o.call(o.currentStep))
		n++
		o.currentStep += o.step
	}

	return n, nil
}
