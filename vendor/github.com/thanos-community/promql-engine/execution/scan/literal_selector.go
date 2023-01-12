// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package scan

import (
	"context"
	"fmt"
	"sync"

	"github.com/thanos-community/promql-engine/execution/model"
	"github.com/thanos-community/promql-engine/query"

	"github.com/prometheus/prometheus/model/labels"
)

// numberLiteralSelector returns []model.StepVector with same sample value across time range.
type numberLiteralSelector struct {
	vectorPool *model.VectorPool

	numSteps    int
	mint        int64
	maxt        int64
	step        int64
	currentStep int64
	series      []labels.Labels
	once        sync.Once

	val float64
}

func NewNumberLiteralSelector(pool *model.VectorPool, opts *query.Options, val float64) *numberLiteralSelector {
	return &numberLiteralSelector{
		vectorPool:  pool,
		numSteps:    opts.NumSteps(),
		mint:        opts.Start.UnixMilli(),
		maxt:        opts.End.UnixMilli(),
		step:        opts.Step.Milliseconds(),
		currentStep: opts.Start.UnixMilli(),
		val:         val,
	}
}

func (o *numberLiteralSelector) Explain() (me string, next []model.VectorOperator) {
	return fmt.Sprintf("[*numberLiteralSelector] %v", o.val), nil
}

func (o *numberLiteralSelector) Series(context.Context) ([]labels.Labels, error) {
	o.loadSeries()
	return o.series, nil
}

func (o *numberLiteralSelector) GetPool() *model.VectorPool {
	return o.vectorPool
}

func (o *numberLiteralSelector) Next(ctx context.Context) ([]model.StepVector, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if o.currentStep > o.maxt {
		return nil, nil
	}

	o.loadSeries()

	vectors := o.vectorPool.GetVectorBatch()
	ts := o.currentStep
	for currStep := 0; currStep < o.numSteps && ts <= o.maxt; currStep++ {
		if len(vectors) <= currStep {
			vectors = append(vectors, o.vectorPool.GetStepVector(ts))
		}

		vectors[currStep].SampleIDs = append(vectors[currStep].SampleIDs, uint64(0))
		vectors[currStep].Samples = append(vectors[currStep].Samples, o.val)

		ts += o.step
	}

	// For instant queries, set the step to a positive value
	// so that the operator can terminate.
	if o.step == 0 {
		o.step = 1
	}
	o.currentStep += o.step * int64(o.numSteps)

	return vectors, nil
}

func (o *numberLiteralSelector) loadSeries() {
	// If number literal is included within function, []labels.labels must be initialized.
	o.once.Do(func() {
		o.series = make([]labels.Labels, 1)
		o.vectorPool.SetStepSize(len(o.series))
	})
}
