// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package function

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/parse"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/extlabels"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

func NewFunctionOperator(funcExpr *logicalplan.FunctionCall, nextOps []model.VectorOperator, stepsBatch int, opts *query.Options) (model.VectorOperator, error) {
	// Some functions need to be handled in special operators
	switch funcExpr.Func.Name {
	case "scalar":
		return newScalarOperator(nextOps[0], opts), nil
	case "timestamp":
		return newTimestampOperator(nextOps[0], opts), nil
	case "label_join", "label_replace":
		return newRelabelOperator(nextOps[0], funcExpr, opts), nil
	case "absent":
		return newAbsentOperator(funcExpr, nextOps[0], opts), nil
	case "histogram_quantile", "histogram_fraction":
		return newHistogramOperator(funcExpr, nextOps, stepsBatch, opts), nil
	}

	// Short-circuit functions that take no args. Their only input is the step's timestamp.
	if len(nextOps) == 0 {
		return newNoArgsFunctionOperator(funcExpr, stepsBatch, opts)
	}
	// All remaining functions
	return newInstantVectorFunctionOperator(funcExpr, nextOps, stepsBatch, opts)
}

func newNoArgsFunctionOperator(funcExpr *logicalplan.FunctionCall, stepsBatch int, opts *query.Options) (model.VectorOperator, error) {
	call, ok := noArgFuncs[funcExpr.Func.Name]
	if !ok {
		return nil, parse.UnknownFunctionError(funcExpr.Func.Name)
	}

	interval := opts.Step.Milliseconds()
	// We set interval to be at least 1.
	if interval == 0 {
		interval = 1
	}

	op := &noArgFunctionOperator{
		currentStep: opts.Start.UnixMilli(),
		mint:        opts.Start.UnixMilli(),
		maxt:        opts.End.UnixMilli(),
		step:        interval,
		stepsBatch:  stepsBatch,
		funcExpr:    funcExpr,
		call:        call,
	}

	switch funcExpr.Func.Name {
	case "pi", "time":
		op.sampleIDs = []uint64{0}
	default:
		// Other functions require non-nil labels.
		op.series = []labels.Labels{{}}
		op.sampleIDs = []uint64{0}
	}

	return telemetry.NewOperator(telemetry.NewTelemetry(op, opts), op), nil
}

// functionOperator returns []model.StepVector after processing input with desired function.
type functionOperator struct {
	funcExpr *logicalplan.FunctionCall
	series   []labels.Labels
	once     sync.Once

	vectorIndex int
	nextOps     []model.VectorOperator
	stepsBatch  int

	call         functionCall
	scalarPoints [][]float64
	scalarBuf    []model.StepVector
}

func newInstantVectorFunctionOperator(funcExpr *logicalplan.FunctionCall, nextOps []model.VectorOperator, stepsBatch int, opts *query.Options) (model.VectorOperator, error) {
	call, ok := instantVectorFuncs[funcExpr.Func.Name]
	if !ok {
		return nil, parse.UnknownFunctionError(funcExpr.Func.Name)
	}

	scalarPoints := make([][]float64, stepsBatch)
	for i := range stepsBatch {
		scalarPoints[i] = make([]float64, len(nextOps)-1)
	}
	f := &functionOperator{
		nextOps:      nextOps,
		call:         call,
		funcExpr:     funcExpr,
		vectorIndex:  0,
		stepsBatch:   stepsBatch,
		scalarPoints: scalarPoints,
	}

	for i := range funcExpr.Args {
		if funcExpr.Args[i].ReturnType() == parser.ValueTypeVector {
			f.vectorIndex = i
			break
		}
	}

	// Check selector type.
	switch funcExpr.Args[f.vectorIndex].ReturnType() {
	case parser.ValueTypeVector, parser.ValueTypeScalar:
		return telemetry.NewOperator(telemetry.NewTelemetry(f, opts), f), nil
	default:
		return nil, errors.Wrapf(parse.ErrNotImplemented, "got %s:", funcExpr.String())
	}
}

func (o *functionOperator) Explain() (next []model.VectorOperator) {
	return o.nextOps
}

func (o *functionOperator) String() string {
	return fmt.Sprintf("[function] %v(%v)", o.funcExpr.Func.Name, o.funcExpr.Args)
}

func (o *functionOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	if err := o.loadSeries(ctx); err != nil {
		return nil, err
	}
	return o.series, nil
}

func (o *functionOperator) Next(ctx context.Context, buf []model.StepVector) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}
	if err := o.loadSeries(ctx); err != nil {
		return 0, err
	}

	// Process non-variadic single/multi-arg instant vector and scalar input functions.
	// Call next on vector input.
	n, err := o.nextOps[o.vectorIndex].Next(ctx, buf)
	if err != nil {
		return 0, err
	}

	if n == 0 {
		return 0, nil
	}

	scalarIndex := 0
	for i := range o.nextOps {
		if i == o.vectorIndex {
			continue
		}

		scalarN, err := o.nextOps[i].Next(ctx, o.scalarBuf)
		if err != nil {
			return 0, err
		}

		for batchIndex := range n {
			val := math.NaN()
			if batchIndex < scalarN && len(o.scalarBuf[batchIndex].Samples) > 0 {
				val = o.scalarBuf[batchIndex].Samples[0]
			}
			o.scalarPoints[batchIndex][scalarIndex] = val
		}
		scalarIndex++
	}

	for batchIndex := range n {
		vector := &buf[batchIndex]
		i := 0
		for i < len(vector.Samples) {
			if v, ok := o.call(vector.Samples[i], nil, o.scalarPoints[batchIndex]...); ok {
				vector.Samples[i] = v
				i++
			} else {
				// This operator modifies samples directly in the input vector to avoid allocations.
				// In case of an invalid output sample, we need to do an in-place removal of the input sample.
				vector.RemoveSample(i)
			}
		}

		i = 0
		for i < len(vector.Histograms) {
			v, ok := o.call(0., vector.Histograms[i], o.scalarPoints[batchIndex]...)
			// This operator modifies samples directly in the input vector to avoid allocations.
			// All current functions for histograms produce a float64 sample. It's therefore safe to
			// always remove the input histogram so that it does not propagate to the output.
			sampleID := vector.HistogramIDs[i]
			vector.RemoveHistogram(i)
			if ok {
				vector.AppendSample(sampleID, v)
			}
		}
	}

	return n, nil
}

func (o *functionOperator) loadSeries(ctx context.Context) error {
	var err error
	o.once.Do(func() {

		o.scalarBuf = make([]model.StepVector, o.stepsBatch)

		if o.funcExpr.Func.Name == "vector" {
			o.series = []labels.Labels{labels.New()}
			return
		}

		series, loadErr := o.nextOps[o.vectorIndex].Series(ctx)
		if loadErr != nil {
			err = loadErr
			return
		}
		o.series = make([]labels.Labels, len(series))

		var b labels.ScratchBuilder
		for i, s := range series {
			lbls := extlabels.DropReserved(s, b)
			o.series[i] = lbls
		}
	})

	return err
}
