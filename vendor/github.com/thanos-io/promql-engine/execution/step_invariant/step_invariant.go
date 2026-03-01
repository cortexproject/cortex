// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package step_invariant

import (
	"context"
	"sync"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/labels"
)

type stepInvariantOperator struct {
	next        model.VectorOperator
	cacheResult bool

	seriesOnce      sync.Once
	series          []labels.Labels
	cacheVectorOnce sync.Once
	cachedVector    model.StepVector

	mint        int64
	maxt        int64
	step        int64
	currentStep int64
	stepsBatch  int
}

func (u *stepInvariantOperator) Explain() (next []model.VectorOperator) {
	return []model.VectorOperator{u.next}
}

func (u *stepInvariantOperator) String() string {
	return "[stepInvariant]"
}

func NewStepInvariantOperator(
	next model.VectorOperator,
	expr logicalplan.Node,
	opts *query.Options,
) (model.VectorOperator, error) {
	// We set interval to be at least 1.
	u := &stepInvariantOperator{
		next:        next,
		currentStep: opts.Start.UnixMilli(),
		mint:        opts.Start.UnixMilli(),
		maxt:        opts.End.UnixMilli(),
		step:        opts.Step.Milliseconds(),
		stepsBatch:  opts.StepsBatch,
		cacheResult: true,
	}
	if u.step == 0 {
		u.step = 1
	}
	// We do not duplicate results for range selectors since result is a matrix
	// with their unique timestamps which does not depend on the step.
	switch expr.(type) {
	case *logicalplan.MatrixSelector, *logicalplan.Subquery:
		u.cacheResult = false
	}

	return telemetry.NewOperator(telemetry.NewStepInvariantTelemetry(u, opts), u), nil
}

func (u *stepInvariantOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	var err error
	u.seriesOnce.Do(func() {
		u.series, err = u.next.Series(ctx)
	})
	if err != nil {
		return nil, err
	}
	return u.series, nil
}

func (u *stepInvariantOperator) Next(ctx context.Context, buf []model.StepVector) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	if u.currentStep > u.maxt {
		return 0, nil
	}

	if !u.cacheResult {
		return u.next.Next(ctx, buf)
	}

	if err := u.cacheInputVector(ctx); err != nil {
		return 0, err
	}

	n := 0
	maxSteps := min(u.stepsBatch, len(buf))

	for i := 0; i < maxSteps && u.currentStep <= u.maxt; i++ {
		buf[n].Reset(u.currentStep)
		buf[n].AppendSamples(u.cachedVector.SampleIDs, u.cachedVector.Samples)
		buf[n].AppendHistograms(u.cachedVector.HistogramIDs, u.cachedVector.Histograms)
		n++
		u.currentStep += u.step
	}

	return n, nil
}

func (u *stepInvariantOperator) cacheInputVector(ctx context.Context) error {
	var err error
	u.cacheVectorOnce.Do(func() {
		// Create a temporary buffer for reading one vector
		tempBuf := make([]model.StepVector, 1)
		n, readErr := u.next.Next(ctx, tempBuf)
		if readErr != nil {
			err = readErr
			return
		}

		if n == 0 || (len(tempBuf[0].Samples) == 0 && len(tempBuf[0].Histograms) == 0) {
			return
		}

		// Make sure we only have exactly one step vector.
		if n != 1 {
			err = errors.New("unexpected number of samples")
			return
		}

		// Copy the evaluated step vector.
		// The timestamp of the vector is not relevant since we will produce
		// new output vectors with the current step's timestamp.
		u.cachedVector = model.StepVector{T: 0}
		u.cachedVector.AppendSamples(tempBuf[0].SampleIDs, tempBuf[0].Samples)
		u.cachedVector.AppendHistograms(tempBuf[0].HistogramIDs, tempBuf[0].Histograms)
	})
	return err
}
