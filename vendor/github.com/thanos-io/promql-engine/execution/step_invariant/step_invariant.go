// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package step_invariant

import (
	"context"
	"sync"
	"time"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/labels"
)

type stepInvariantOperator struct {
	vectorPool  *model.VectorPool
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
	telemetry.OperatorTelemetry
}

func (u *stepInvariantOperator) Explain() (next []model.VectorOperator) {
	return []model.VectorOperator{u.next}
}

func (u *stepInvariantOperator) String() string {
	return "[stepInvariant]"
}

func NewStepInvariantOperator(
	pool *model.VectorPool,
	next model.VectorOperator,
	expr logicalplan.Node,
	opts *query.Options,
) (model.VectorOperator, error) {
	// We set interval to be at least 1.
	u := &stepInvariantOperator{
		vectorPool:  pool,
		next:        next,
		currentStep: opts.Start.UnixMilli(),
		mint:        opts.Start.UnixMilli(),
		maxt:        opts.End.UnixMilli(),
		step:        opts.Step.Milliseconds(),
		stepsBatch:  opts.StepsBatch,
		cacheResult: true,
	}
	u.OperatorTelemetry = telemetry.NewStepInvariantTelemetry(u, opts)
	if u.step == 0 {
		u.step = 1
	}
	// We do not duplicate results for range selectors since result is a matrix
	// with their unique timestamps which does not depend on the step.
	switch expr.(type) {
	case *logicalplan.MatrixSelector, *logicalplan.Subquery:
		u.cacheResult = false
	}

	return u, nil
}

func (u *stepInvariantOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	start := time.Now()
	defer func() { u.AddExecutionTimeTaken(time.Since(start)) }()

	var err error
	u.seriesOnce.Do(func() {
		u.series, err = u.next.Series(ctx)
		u.vectorPool.SetStepSize(len(u.series))
	})
	if err != nil {
		return nil, err
	}
	return u.series, nil
}

func (u *stepInvariantOperator) GetPool() *model.VectorPool {
	return u.vectorPool
}

func (u *stepInvariantOperator) Next(ctx context.Context) ([]model.StepVector, error) {
	start := time.Now()
	defer func() { u.AddExecutionTimeTaken(time.Since(start)) }()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if u.currentStep > u.maxt {
		return nil, nil
	}

	if !u.cacheResult {
		return u.next.Next(ctx)
	}

	if err := u.cacheInputVector(ctx); err != nil {
		return nil, err
	}

	result := u.vectorPool.GetVectorBatch()
	for i := 0; i < u.stepsBatch && u.currentStep <= u.maxt; i++ {
		outVector := u.vectorPool.GetStepVector(u.currentStep)
		outVector.AppendSamples(u.vectorPool, u.cachedVector.SampleIDs, u.cachedVector.Samples)
		outVector.AppendHistograms(u.vectorPool, u.cachedVector.HistogramIDs, u.cachedVector.Histograms)
		result = append(result, outVector)
		u.currentStep += u.step
	}

	return result, nil
}

func (u *stepInvariantOperator) cacheInputVector(ctx context.Context) error {
	var err error
	var in []model.StepVector
	u.cacheVectorOnce.Do(func() {
		in, err = u.next.Next(ctx)
		if err != nil {
			return
		}
		defer u.next.GetPool().PutVectors(in)

		if len(in) == 0 || (len(in[0].Samples) == 0 && len(in[0].Histograms) == 0) {
			return
		}

		// Make sure we only have exactly one step vector.
		if len(in) != 1 {
			err = errors.New("unexpected number of samples")
			return
		}

		// Copy the evaluated step vector.
		// The timestamp of the vector is not relevant since we will produce
		// new output vectors with the current step's timestamp.
		u.cachedVector = u.vectorPool.GetStepVector(0)
		u.cachedVector.AppendSamples(u.vectorPool, in[0].SampleIDs, in[0].Samples)
		u.cachedVector.AppendHistograms(u.vectorPool, in[0].HistogramIDs, in[0].Histograms)
		u.next.GetPool().PutStepVector(in[0])
	})
	return err
}
