// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package binary

import (
	"context"
	"fmt"
	"sync"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/extlabels"
	"github.com/thanos-io/promql-engine/query"
	"github.com/thanos-io/promql-engine/warnings"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

// scalarOperator evaluates expressions where one operand is a scalarOperator.
type scalarOperator struct {
	seriesOnce sync.Once
	series     []labels.Labels

	pool   *model.VectorPool
	lhs    model.VectorOperator
	rhs    model.VectorOperator
	opType parser.ItemType

	// If true then return the comparison result as 0/1.
	returnBool bool

	lhsType parser.ValueType
	rhsType parser.ValueType
}

func NewScalar(
	pool *model.VectorPool,
	lhs model.VectorOperator,
	rhs model.VectorOperator,
	lhsType parser.ValueType,
	rhsType parser.ValueType,
	opType parser.ItemType,
	returnBool bool,
	opts *query.Options,
) (model.VectorOperator, error) {
	op := &scalarOperator{
		pool:       pool,
		lhs:        lhs,
		rhs:        rhs,
		lhsType:    lhsType,
		rhsType:    rhsType,
		opType:     opType,
		returnBool: returnBool,
	}

	return telemetry.NewOperator(telemetry.NewTelemetry(op, opts), op), nil
}

func (o *scalarOperator) Explain() (next []model.VectorOperator) {
	return []model.VectorOperator{o.lhs, o.rhs}
}

func (o *scalarOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	var err error
	o.seriesOnce.Do(func() { err = o.loadSeries(ctx) })
	if err != nil {
		return nil, err
	}
	return o.series, nil
}

func (o *scalarOperator) String() string {
	return fmt.Sprintf("[vectorScalarBinary] %s", parser.ItemTypeStr[o.opType])
}

func (o *scalarOperator) Next(ctx context.Context) ([]model.StepVector, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	var err error
	o.seriesOnce.Do(func() { err = o.loadSeries(ctx) })
	if err != nil {
		return nil, err
	}

	var lhs []model.StepVector
	var lerrChan = make(chan error, 1)
	go func() {
		var err error
		lhs, err = o.lhs.Next(ctx)
		if err != nil {
			lerrChan <- err
		}
		close(lerrChan)
	}()

	rhs, rerr := o.rhs.Next(ctx)
	lerr := <-lerrChan
	if rerr != nil {
		return nil, rerr
	}
	if lerr != nil {
		return nil, lerr
	}

	// TODO(fpetkovski): When one operator becomes empty,
	// we might want to drain or close the other one.
	// We don't have a concept of closing an operator yet.
	if len(lhs) == 0 || len(rhs) == 0 {
		return nil, nil
	}

	batch := o.pool.GetVectorBatch()
	for i := range lhs {
		if i < len(rhs) {
			step := o.execBinaryOperation(ctx, lhs[i], rhs[i])
			batch = append(batch, step)
			o.rhs.GetPool().PutStepVector(rhs[i])
		}
		o.lhs.GetPool().PutStepVector(lhs[i])
	}
	o.lhs.GetPool().PutVectors(lhs)
	o.rhs.GetPool().PutVectors(rhs)

	return batch, nil

}

func (o *scalarOperator) GetPool() *model.VectorPool {
	return o.pool
}

func (o *scalarOperator) loadSeries(ctx context.Context) error {
	vectorSide := o.lhs
	if o.lhsType == parser.ValueTypeScalar {
		vectorSide = o.rhs
	}
	vectorSeries, err := vectorSide.Series(ctx)
	if err != nil {
		return err
	}

	series := make([]labels.Labels, len(vectorSeries))
	var b labels.ScratchBuilder
	for i := range vectorSeries {
		if !vectorSeries[i].IsEmpty() {
			lbls := vectorSeries[i]
			if shouldDropMetricName(o.opType, o.returnBool) {
				lbls = extlabels.DropReserved(lbls, b)
			}
			series[i] = lbls
		} else {
			series[i] = vectorSeries[i]
		}
	}

	o.series = series
	return nil
}

func (o *scalarOperator) execBinaryOperation(ctx context.Context, lhs, rhs model.StepVector) model.StepVector {
	ts := lhs.T
	step := o.pool.GetStepVector(ts)

	scalar, other := lhs, rhs
	if o.lhsType != parser.ValueTypeScalar {
		scalar, other = rhs, lhs
	}

	var (
		v    float64
		h    *histogram.FloatHistogram
		keep bool
		err  error
	)
	for i, otherVal := range other.Samples {
		scalarVal := scalar.Samples[0]

		if o.lhsType == parser.ValueTypeScalar {
			v, _, keep, err = binOp(o.opType, scalarVal, otherVal, nil, nil)
		} else {
			v, _, keep, err = binOp(o.opType, otherVal, scalarVal, nil, nil)
		}
		if err != nil {
			warnings.AddToContext(err, ctx)
			continue
		}
		// in comparison operations between scalars and vectors, the vectors are filtered, regardless if lhs or rhs
		if keep && o.opType.IsComparisonOperator() && (o.lhsType == parser.ValueTypeVector || o.rhsType == parser.ValueTypeVector) {
			v = otherVal
		}
		if o.returnBool {
			v = 0.0
			if keep {
				v = 1.0
			}
		} else if !keep {
			continue
		}
		step.AppendSample(o.pool, other.SampleIDs[i], v)
	}
	for i, otherVal := range other.Histograms {
		scalarVal := scalar.Samples[0]

		if o.lhsType == parser.ValueTypeScalar {
			_, h, _, err = binOp(o.opType, scalarVal, 0., nil, otherVal)
		} else {
			_, h, _, err = binOp(o.opType, 0., scalarVal, otherVal, nil)
		}
		if err != nil {
			warnings.AddToContext(err, ctx)
			continue
		}
		step.AppendHistogram(o.pool, other.HistogramIDs[i], h)
	}

	return step
}
