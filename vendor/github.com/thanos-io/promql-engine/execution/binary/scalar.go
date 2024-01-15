// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package binary

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/extlabels"
	"github.com/thanos-io/promql-engine/query"
)

type ScalarSide int

const (
	ScalarSideBoth ScalarSide = iota
	ScalarSideLeft
	ScalarSideRight
)

// scalarOperator evaluates expressions where one operand is a scalarOperator.
type scalarOperator struct {
	model.OperatorTelemetry

	seriesOnce sync.Once
	series     []labels.Labels

	pool          *model.VectorPool
	scalar        model.VectorOperator
	next          model.VectorOperator
	opType        parser.ItemType
	getOperands   getOperandsFunc
	operandValIdx int
	floatOp       operation
	histOp        histogramFloatOperation

	// If true then return the comparison result as 0/1.
	returnBool bool

	// Keep the result if both sides are scalars.
	bothScalars bool
}

func NewScalar(
	pool *model.VectorPool,
	next model.VectorOperator,
	scalar model.VectorOperator,
	op parser.ItemType,
	scalarSide ScalarSide,
	returnBool bool,
	opts *query.Options,
) (*scalarOperator, error) {
	binaryOperation, err := newOperation(op, scalarSide != ScalarSideBoth)
	if err != nil {
		return nil, err
	}
	// operandValIdx 0 means to get lhs as the return value
	// while 1 means to get rhs as the return value.
	operandValIdx := 0
	getOperands := getOperandsScalarRight
	if scalarSide == ScalarSideLeft {
		getOperands = getOperandsScalarLeft
		operandValIdx = 1
	}

	return &scalarOperator{
		OperatorTelemetry: model.NewTelemetry("[vectorScalarBinary]", opts.EnableAnalysis),

		pool:          pool,
		next:          next,
		scalar:        scalar,
		floatOp:       binaryOperation,
		histOp:        getHistogramFloatOperation(op, scalarSide),
		opType:        op,
		getOperands:   getOperands,
		operandValIdx: operandValIdx,
		returnBool:    returnBool,
		bothScalars:   scalarSide == ScalarSideBoth,
	}, nil

}

func (o *scalarOperator) Explain() (me string, next []model.VectorOperator) {
	return fmt.Sprintf("[vectorScalarBinary] %s", parser.ItemTypeStr[o.opType]), []model.VectorOperator{o.next, o.scalar}
}

func (o *scalarOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	start := time.Now()
	defer func() { o.AddExecutionTimeTaken(time.Since(start)) }()

	var err error
	o.seriesOnce.Do(func() { err = o.loadSeries(ctx) })
	if err != nil {
		return nil, err
	}
	return o.series, nil
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
	if in == nil {
		return nil, nil
	}
	o.seriesOnce.Do(func() { err = o.loadSeries(ctx) })
	if err != nil {
		return nil, err
	}

	scalarIn, err := o.scalar.Next(ctx)
	if err != nil {
		return nil, err
	}

	out := o.pool.GetVectorBatch()
	for v, vector := range in {
		step := o.pool.GetStepVector(vector.T)
		scalarVal := math.NaN()
		if len(scalarIn) > v && len(scalarIn[v].Samples) > 0 {
			scalarVal = scalarIn[v].Samples[0]
		}

		for i := range vector.Samples {
			operands := o.getOperands(vector, i, scalarVal)
			val, keep := o.floatOp(operands, o.operandValIdx)
			if o.returnBool {
				if !o.bothScalars {
					val = 0.0
					if keep {
						val = 1.0
					}
				}
			} else if !keep {
				continue
			}
			step.AppendSample(o.pool, vector.SampleIDs[i], val)
		}

		for i := range vector.HistogramIDs {
			val := o.histOp(vector.Histograms[i], scalarVal)
			if val != nil {
				step.AppendHistogram(o.pool, vector.HistogramIDs[i], val)
			}
		}

		out = append(out, step)
		o.next.GetPool().PutStepVector(vector)
	}

	for i := range scalarIn {
		o.scalar.GetPool().PutStepVector(scalarIn[i])
	}

	o.next.GetPool().PutVectors(in)
	o.scalar.GetPool().PutVectors(scalarIn)

	return out, nil
}

func (o *scalarOperator) GetPool() *model.VectorPool {
	return o.pool
}

func (o *scalarOperator) loadSeries(ctx context.Context) error {
	vectorSeries, err := o.next.Series(ctx)
	if err != nil {
		return err
	}
	series := make([]labels.Labels, len(vectorSeries))
	b := labels.ScratchBuilder{}
	for i := range vectorSeries {
		if !vectorSeries[i].IsEmpty() {
			lbls := vectorSeries[i]
			if shouldDropMetricName(o.opType, o.returnBool) {
				lbls, _ = extlabels.DropMetricName(lbls, b)
			}
			series[i] = lbls
		} else {
			series[i] = vectorSeries[i]
		}
	}

	o.series = series
	return nil
}

type getOperandsFunc func(v model.StepVector, i int, scalar float64) [2]float64

func getOperandsScalarLeft(v model.StepVector, i int, scalar float64) [2]float64 {
	return [2]float64{scalar, v.Samples[i]}
}

func getOperandsScalarRight(v model.StepVector, i int, scalar float64) [2]float64 {
	return [2]float64{v.Samples[i], scalar}
}
