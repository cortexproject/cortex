// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package step_invariant

import (
	"context"
	"sync"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-community/promql-engine/internal/prometheus/parser"

	"github.com/thanos-community/promql-engine/execution/model"
	"github.com/thanos-community/promql-engine/query"
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
}

func (u *stepInvariantOperator) Explain() (me string, next []model.VectorOperator) {
	return "[*stepInvariantOperator]", []model.VectorOperator{u.next}
}

func NewStepInvariantOperator(
	pool *model.VectorPool,
	next model.VectorOperator,
	expr parser.Expr,
	opts *query.Options,
	stepsBatch int,
) (model.VectorOperator, error) {
	interval := opts.Step.Milliseconds()
	// We set interval to be at least 1.
	if interval == 0 {
		interval = 1
	}
	u := &stepInvariantOperator{
		vectorPool:  pool,
		next:        next,
		currentStep: opts.Start.UnixMilli(),
		mint:        opts.Start.UnixMilli(),
		maxt:        opts.End.UnixMilli(),
		step:        interval,
		stepsBatch:  stepsBatch,
		cacheResult: true,
	}
	// We do not duplicate results for range selectors since result is a matrix
	// with their unique timestamps which does not depend on the step.
	switch expr.(type) {
	case *parser.MatrixSelector, *parser.SubqueryExpr:
		u.cacheResult = false
	}

	return u, nil
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

func (u *stepInvariantOperator) GetPool() *model.VectorPool {
	return u.vectorPool
}

func (u *stepInvariantOperator) Next(ctx context.Context) ([]model.StepVector, error) {
	if u.currentStep > u.maxt {
		return nil, nil
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if !u.cacheResult {
		return u.next.Next(ctx)
	}

	if err := u.cacheInputVector(ctx); err != nil {
		return nil, err
	}

	if len(u.cachedVector.Samples) == 0 {
		return nil, nil
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

		if len(in) == 0 || len(in[0].Samples) == 0 {
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
