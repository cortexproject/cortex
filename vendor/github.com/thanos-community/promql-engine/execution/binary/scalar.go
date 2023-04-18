// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package binary

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-community/promql-engine/internal/prometheus/parser"

	"github.com/thanos-community/promql-engine/execution/function"
	"github.com/thanos-community/promql-engine/execution/model"
)

type ScalarSide int

const (
	ScalarSideBoth ScalarSide = iota
	ScalarSideLeft
	ScalarSideRight
)

// scalarOperator evaluates expressions where one operand is a scalarOperator.
type scalarOperator struct {
	seriesOnce sync.Once
	series     []labels.Labels

	pool          *model.VectorPool
	scalar        model.VectorOperator
	next          model.VectorOperator
	getOperands   getOperandsFunc
	operandValIdx int
	operation     operation
	opType        parser.ItemType

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
		pool:          pool,
		next:          next,
		scalar:        scalar,
		operation:     binaryOperation,
		opType:        op,
		getOperands:   getOperands,
		operandValIdx: operandValIdx,
		returnBool:    returnBool,
		bothScalars:   scalarSide == ScalarSideBoth,
	}, nil
}

func (o *scalarOperator) Explain() (me string, next []model.VectorOperator) {
	return fmt.Sprintf("[*scalarOperator] %s", parser.ItemTypeStr[o.opType]), []model.VectorOperator{o.next, o.scalar}
}

func (o *scalarOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	var err error
	o.seriesOnce.Do(func() { err = o.loadSeries(ctx) })
	if err != nil {
		return nil, err
	}
	return o.series, nil
}

func (o *scalarOperator) Next(ctx context.Context) ([]model.StepVector, error) {
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
		for i := range vector.Samples {
			scalarVal := math.NaN()
			if len(scalarIn) > v && len(scalarIn[v].Samples) > 0 {
				scalarVal = scalarIn[v].Samples[0]
			}

			operands := o.getOperands(vector, i, scalarVal)
			val, keep := o.operation(operands, o.operandValIdx)
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
	for i := range vectorSeries {
		if vectorSeries[i] != nil {
			lbls := vectorSeries[i]
			if shouldDropMetricName(o.opType, o.returnBool) {
				lbls, _ = function.DropMetricName(lbls.Copy())
			}
			series[i] = lbls
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
