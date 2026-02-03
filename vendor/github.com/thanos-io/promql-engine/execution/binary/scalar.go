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
	lhs        model.VectorOperator
	rhs        model.VectorOperator
	lhsType    parser.ValueType
	rhsType    parser.ValueType
	opType     parser.ItemType
	returnBool bool
	stepsBatch int

	once   sync.Once
	series []labels.Labels

	lhsBuf []model.StepVector
	rhsBuf []model.StepVector
}

func NewScalar(
	lhs model.VectorOperator,
	rhs model.VectorOperator,
	lhsType parser.ValueType,
	rhsType parser.ValueType,
	opType parser.ItemType,
	returnBool bool,
	opts *query.Options,
) (model.VectorOperator, error) {
	op := &scalarOperator{
		lhs:        lhs,
		rhs:        rhs,
		lhsType:    lhsType,
		rhsType:    rhsType,
		opType:     opType,
		returnBool: returnBool,
		stepsBatch: opts.StepsBatch,
	}

	return telemetry.NewOperator(telemetry.NewTelemetry(op, opts), op), nil
}

func (o *scalarOperator) Explain() (next []model.VectorOperator) {
	return []model.VectorOperator{o.lhs, o.rhs}
}

func (o *scalarOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	var err error
	o.once.Do(func() { err = o.loadSeries(ctx) })
	if err != nil {
		return nil, err
	}
	return o.series, nil
}

func (o *scalarOperator) String() string {
	return fmt.Sprintf("[vectorScalarBinary] %s", parser.ItemTypeStr[o.opType])
}

func (o *scalarOperator) Next(ctx context.Context, buf []model.StepVector) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	var err error
	o.once.Do(func() { err = o.loadSeries(ctx) })
	if err != nil {
		return 0, err
	}

	var lhsN int
	var lerrChan = make(chan error, 1)
	go func() {
		var err error
		lhsN, err = o.lhs.Next(ctx, o.lhsBuf)
		if err != nil {
			lerrChan <- err
		}
		close(lerrChan)
	}()

	rhsN, rerr := o.rhs.Next(ctx, o.rhsBuf)
	lerr := <-lerrChan
	if rerr != nil {
		return 0, rerr
	}
	if lerr != nil {
		return 0, lerr
	}

	// TODO(fpetkovski): When one operator becomes empty,
	// we might want to drain or close the other one.
	// We don't have a concept of closing an operator yet.
	if lhsN == 0 || rhsN == 0 {
		return 0, nil
	}

	n := 0
	minN := min(rhsN, lhsN)

	for i := 0; i < minN && n < len(buf); i++ {
		o.execBinaryOperation(ctx, o.lhsBuf[i], o.rhsBuf[i], &buf[n])
		n++
	}

	return n, nil
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

	// Pre-allocate buffers with appropriate inner slice capacities.
	// One side is a scalar (1 sample), the other is a vector (len(vectorSeries) samples).
	o.lhsBuf = make([]model.StepVector, o.stepsBatch)
	o.rhsBuf = make([]model.StepVector, o.stepsBatch)

	var lhsSeriesCount, rhsSeriesCount int
	if o.lhsType == parser.ValueTypeScalar {
		lhsSeriesCount = 1
		rhsSeriesCount = len(vectorSeries)
	} else {
		lhsSeriesCount = len(vectorSeries)
		rhsSeriesCount = 1
	}

	// Pre-allocate float sample slices; histogram slices will grow on demand.
	for i := range o.lhsBuf {
		o.lhsBuf[i].SampleIDs = make([]uint64, 0, lhsSeriesCount)
		o.lhsBuf[i].Samples = make([]float64, 0, lhsSeriesCount)
	}
	for i := range o.rhsBuf {
		o.rhsBuf[i].SampleIDs = make([]uint64, 0, rhsSeriesCount)
		o.rhsBuf[i].Samples = make([]float64, 0, rhsSeriesCount)
	}

	return nil
}

func (o *scalarOperator) execBinaryOperation(ctx context.Context, lhs, rhs model.StepVector, step *model.StepVector) {
	ts := lhs.T
	step.Reset(ts)

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
	var warn warnings.Warnings
	sampleHint := len(other.Samples)
	for i, otherVal := range other.Samples {
		scalarVal := scalar.Samples[0]

		if o.lhsType == parser.ValueTypeScalar {
			v, _, keep, warn, err = binOp(o.opType, scalarVal, otherVal, nil, nil)
		} else {
			v, _, keep, warn, err = binOp(o.opType, otherVal, scalarVal, nil, nil)
		}
		if err != nil {
			warnings.AddToContext(err, ctx)
			continue
		}
		if warn != 0 {
			emitBinaryOpWarnings(ctx, warn, o.opType)
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
		step.AppendSampleWithSizeHint(other.SampleIDs[i], v, sampleHint)
	}
	histogramHint := len(other.Histograms)
	for i, otherVal := range other.Histograms {
		scalarVal := scalar.Samples[0]

		if o.lhsType == parser.ValueTypeScalar {
			_, h, keep, warn, err = binOp(o.opType, scalarVal, 0., nil, otherVal)
		} else {
			_, h, keep, warn, err = binOp(o.opType, 0., scalarVal, otherVal, nil)
		}
		if err != nil {
			warnings.AddToContext(err, ctx)
			continue
		}
		if warn != 0 {
			emitBinaryOpWarnings(ctx, warn, o.opType)
		}
		if !keep {
			continue
		}
		step.AppendHistogramWithSizeHint(other.HistogramIDs[i], h, histogramHint)
	}
}
