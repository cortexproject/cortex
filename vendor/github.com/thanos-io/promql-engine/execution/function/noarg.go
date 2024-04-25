// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package function

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/logicalplan"
)

type noArgFunctionOperator struct {
	model.OperatorTelemetry

	mint        int64
	maxt        int64
	step        int64
	currentStep int64
	stepsBatch  int
	funcExpr    *logicalplan.FunctionCall
	call        noArgFunctionCall
	vectorPool  *model.VectorPool
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
	start := time.Now()
	defer func() { o.AddExecutionTimeTaken(time.Since(start)) }()

	return o.series, nil
}

func (o *noArgFunctionOperator) GetPool() *model.VectorPool {
	return o.vectorPool
}

func (o *noArgFunctionOperator) Next(ctx context.Context) ([]model.StepVector, error) {
	start := time.Now()
	defer func() { o.AddExecutionTimeTaken(time.Since(start)) }()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if o.currentStep > o.maxt {
		return nil, nil
	}

	ret := o.vectorPool.GetVectorBatch()
	for i := 0; i < o.stepsBatch && o.currentStep <= o.maxt; i++ {
		sv := o.vectorPool.GetStepVector(o.currentStep)
		sv.Samples = []float64{o.call(o.currentStep)}
		sv.SampleIDs = o.sampleIDs
		ret = append(ret, sv)
		o.currentStep += o.step
	}

	return ret, nil
}
