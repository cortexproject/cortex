// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package exchange

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-community/promql-engine/execution/model"
)

type errorChan chan error

func (c errorChan) getError() error {
	for err := range c {
		if err != nil {
			return err
		}
	}

	return nil
}

// coalesce is a model.VectorOperator that merges input vectors from multiple downstream operators
// into a single output vector.
// coalesce guarantees that samples from different input vectors will be added to the output in the same order
// as the input vectors themselves are provided in NewCoalesce.
type coalesce struct {
	once   sync.Once
	series []labels.Labels

	pool      *model.VectorPool
	wg        sync.WaitGroup
	operators []model.VectorOperator

	// inVectors is an internal per-step cache for references to input vectors.
	inVectors [][]model.StepVector
	// sampleOffsets holds per-operator offsets needed to map an input sample ID to an output sample ID.
	sampleOffsets []uint64
}

func NewCoalesce(pool *model.VectorPool, operators ...model.VectorOperator) model.VectorOperator {
	return &coalesce{
		pool:          pool,
		sampleOffsets: make([]uint64, len(operators)),
		operators:     operators,
		inVectors:     make([][]model.StepVector, len(operators)),
	}
}

func (c *coalesce) Explain() (me string, next []model.VectorOperator) {
	return "[*coalesce]", c.operators
}

func (c *coalesce) GetPool() *model.VectorPool {
	return c.pool
}

func (c *coalesce) Series(ctx context.Context) ([]labels.Labels, error) {
	var err error
	c.once.Do(func() { err = c.loadSeries(ctx) })
	if err != nil {
		return nil, err
	}
	return c.series, nil
}

func (c *coalesce) Next(ctx context.Context) ([]model.StepVector, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	var err error
	c.once.Do(func() { err = c.loadSeries(ctx) })
	if err != nil {
		return nil, err
	}

	var errChan = make(errorChan, len(c.operators))
	for idx, o := range c.operators {
		c.wg.Add(1)
		go func(opIdx int, o model.VectorOperator) {
			defer c.wg.Done()

			in, err := o.Next(ctx)
			if err != nil {
				errChan <- err
				return
			}

			// Map input IDs to output IDs.
			for _, vector := range in {
				for i := range vector.SampleIDs {
					vector.SampleIDs[i] += c.sampleOffsets[opIdx]
				}
			}
			c.inVectors[opIdx] = in
		}(idx, o)
	}
	c.wg.Wait()
	close(errChan)

	if err := errChan.getError(); err != nil {
		return nil, err
	}

	var out []model.StepVector = nil
	for opIdx, vector := range c.inVectors {
		if len(vector) > 0 && out == nil {
			out = c.pool.GetVectorBatch()
			for i := 0; i < len(vector); i++ {
				out = append(out, c.pool.GetStepVector(vector[i].T))
			}
		}

		for i := 0; i < len(vector); i++ {
			out[i].Samples = append(out[i].Samples, vector[i].Samples...)
			out[i].SampleIDs = append(out[i].SampleIDs, vector[i].SampleIDs...)
			c.operators[opIdx].GetPool().PutStepVector(vector[i])
		}
		c.inVectors[opIdx] = nil
		c.operators[opIdx].GetPool().PutVectors(vector)
	}

	if out == nil {
		return nil, nil
	}

	return out, nil
}

func (c *coalesce) loadSeries(ctx context.Context) error {
	var wg sync.WaitGroup
	var numSeries uint64
	allSeries := make([][]labels.Labels, len(c.operators))
	errChan := make(errorChan, len(c.operators))
	for i := 0; i < len(c.operators); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() {
				e := recover()
				if e == nil {
					return
				}

				switch err := e.(type) {
				case error:
					errChan <- errors.Wrapf(err, "unexpected error")
				}

			}()
			series, err := c.operators[i].Series(ctx)
			if err != nil {
				errChan <- err
				return
			}

			allSeries[i] = series
			atomic.AddUint64(&numSeries, uint64(len(series)))
		}(i)
	}
	wg.Wait()
	close(errChan)
	if err := errChan.getError(); err != nil {
		return err
	}

	c.sampleOffsets = make([]uint64, len(c.operators))
	c.series = make([]labels.Labels, 0, numSeries)
	for i, series := range allSeries {
		c.sampleOffsets[i] = uint64(len(c.series))
		c.series = append(c.series, series...)
	}

	c.pool.SetStepSize(len(c.series))
	return nil
}
