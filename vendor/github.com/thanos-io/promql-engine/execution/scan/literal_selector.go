// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package scan

import (
	"context"
	"fmt"
	"sync"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/query"

	"github.com/prometheus/prometheus/model/labels"
)

// numberLiteralSelector returns []model.StepVector with same sample value across time range.
type numberLiteralSelector struct {
	numSteps    int
	mint        int64
	maxt        int64
	step        int64
	currentStep int64
	series      []labels.Labels
	once        sync.Once

	val float64
}

func NewNumberLiteralSelector(opts *query.Options, val float64) model.VectorOperator {
	oper := &numberLiteralSelector{
		numSteps:    opts.NumStepsPerBatch(),
		mint:        opts.Start.UnixMilli(),
		maxt:        opts.End.UnixMilli(),
		step:        opts.Step.Milliseconds(),
		currentStep: opts.Start.UnixMilli(),
		val:         val,
	}

	return telemetry.NewOperator(telemetry.NewTelemetry(oper, opts), oper)
}

func (o *numberLiteralSelector) Explain() (next []model.VectorOperator) {
	return nil
}

func (o *numberLiteralSelector) String() string {
	return fmt.Sprintf("[numberLiteral] %v", o.val)
}

func (o *numberLiteralSelector) Series(context.Context) ([]labels.Labels, error) {
	o.loadSeries()
	return o.series, nil
}

func (o *numberLiteralSelector) Next(ctx context.Context, buf []model.StepVector) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	if o.currentStep > o.maxt {
		return 0, nil
	}

	o.loadSeries()

	ts := o.currentStep
	n := 0
	for n < len(buf) && n < o.numSteps && ts <= o.maxt {
		buf[n].Reset(ts)
		buf[n].AppendSample(0, o.val)

		ts += o.step
		n++
	}

	// For instant queries, set the step to a positive value
	// so that the operator can terminate.
	if o.step == 0 {
		o.step = 1
	}
	o.currentStep += o.step * int64(n)

	return n, nil
}

func (o *numberLiteralSelector) loadSeries() {
	o.once.Do(func() {
		o.series = []labels.Labels{{}}
	})
}
