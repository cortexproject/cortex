// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package exchange

import (
	"context"
	"sync"

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

type coalesceOperator struct {
	once   sync.Once
	series []labels.Labels

	pool          *model.VectorPool
	mu            sync.Mutex
	wg            sync.WaitGroup
	operators     []model.VectorOperator
	sampleOffsets []uint64
}

func NewCoalesce(pool *model.VectorPool, operators ...model.VectorOperator) model.VectorOperator {
	return &coalesceOperator{
		pool:          pool,
		operators:     operators,
		sampleOffsets: make([]uint64, len(operators)),
	}
}

func (c *coalesceOperator) Explain() (me string, next []model.VectorOperator) {
	return "[*coalesceOperator]", c.operators
}

func (c *coalesceOperator) GetPool() *model.VectorPool {
	return c.pool
}

func (c *coalesceOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	var err error
	c.once.Do(func() { err = c.loadSeries(ctx) })
	if err != nil {
		return nil, err
	}
	return c.series, nil
}

func (c *coalesceOperator) Next(ctx context.Context) ([]model.StepVector, error) {
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

	var out []model.StepVector = nil
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
			if in == nil {
				return
			}

			for _, vector := range in {
				for i := range vector.SampleIDs {
					vector.SampleIDs[i] += c.sampleOffsets[opIdx]
				}
			}

			c.mu.Lock()
			defer c.mu.Unlock()

			if len(in) > 0 && out == nil {
				out = c.pool.GetVectorBatch()
				for i := 0; i < len(in); i++ {
					out = append(out, c.pool.GetStepVector(in[i].T))
				}
			}

			for i := 0; i < len(in); i++ {
				if len(in[i].Samples) > 0 {
					out[i].T = in[i].T
				}

				out[i].Samples = append(out[i].Samples, in[i].Samples...)
				out[i].SampleIDs = append(out[i].SampleIDs, in[i].SampleIDs...)
				o.GetPool().PutStepVector(in[i])
			}
			o.GetPool().PutVectors(in)
		}(idx, o)
	}
	c.wg.Wait()
	close(errChan)

	if err := errChan.getError(); err != nil {
		return nil, err
	}

	if out == nil {
		return nil, nil
	}

	return out, nil
}

func (c *coalesceOperator) loadSeries(ctx context.Context) error {
	var wg sync.WaitGroup
	var mu sync.Mutex
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
			mu.Lock()
			numSeries += uint64(len(series))
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	close(errChan)
	if err := errChan.getError(); err != nil {
		return err
	}

	var offset uint64
	c.sampleOffsets = make([]uint64, len(c.operators))
	c.series = make([]labels.Labels, 0, numSeries)
	for i, series := range allSeries {
		c.sampleOffsets[i] = offset
		c.series = append(c.series, series...)
		offset += uint64(len(series))
	}

	c.pool.SetStepSize(len(c.series))
	return nil
}
