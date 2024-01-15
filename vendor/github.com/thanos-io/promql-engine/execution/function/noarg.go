// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package function

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-io/promql-engine/execution/model"
)

type noArgFunctionOperator struct {
	model.OperatorTelemetry

	mint        int64
	maxt        int64
	step        int64
	currentStep int64
	stepsBatch  int
	funcExpr    *parser.Call
	call        noArgFunctionCall
	vectorPool  *model.VectorPool
	series      []labels.Labels
	sampleIDs   []uint64
}

func (o *noArgFunctionOperator) Explain() (me string, next []model.VectorOperator) {
	return fmt.Sprintf("%s %s()", noArgFunctionOperatorName, o.funcExpr.Func.Name), []model.VectorOperator{}
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
