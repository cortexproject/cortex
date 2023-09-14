// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package function

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/parser"
)

type noArgFunctionOperator struct {
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
	model.OperatorTelemetry
}

func (o *noArgFunctionOperator) Analyze() (model.OperatorTelemetry, []model.ObservableVectorOperator) {
	o.SetName("[*noArgFunctionOperator]")
	return o, []model.ObservableVectorOperator{}
}

func (o *noArgFunctionOperator) Explain() (me string, next []model.VectorOperator) {

	return fmt.Sprintf("[*noArgFunctionOperator] %v()", o.funcExpr.Func.Name), []model.VectorOperator{}
}

func (o *noArgFunctionOperator) Series(_ context.Context) ([]labels.Labels, error) {
	return o.series, nil
}

func (o *noArgFunctionOperator) GetPool() *model.VectorPool {
	return o.vectorPool
}

func (o *noArgFunctionOperator) Next(_ context.Context) ([]model.StepVector, error) {
	if o.currentStep > o.maxt {
		return nil, nil
	}
	start := time.Now()
	ret := o.vectorPool.GetVectorBatch()
	for i := 0; i < o.stepsBatch && o.currentStep <= o.maxt; i++ {
		sv := o.vectorPool.GetStepVector(o.currentStep)
		sv.Samples = []float64{o.call(o.currentStep)}
		sv.SampleIDs = o.sampleIDs
		ret = append(ret, sv)
		o.currentStep += o.step
	}
	o.AddExecutionTimeTaken(time.Since(start))

	return ret, nil
}
