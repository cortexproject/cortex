// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package exchange

import (
	"context"
	"math"
	"sync"
	"sync/atomic"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/query"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
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

	wg        sync.WaitGroup
	operators []model.VectorOperator
	batchSize int64

	// inVectors is an internal per-step cache for references to input vectors.
	inVectors [][]model.StepVector
	// sampleOffsets holds per-operator offsets needed to map an input sample ID to an output sample ID.
	sampleOffsets []uint64
	// seriesCounts holds the number of series per operator for pre-allocation.
	seriesCounts []int
	// tempBufs are reusable buffers for reading from operators
	tempBufs [][]model.StepVector
}

func NewCoalesce(opts *query.Options, batchSize int64, operators ...model.VectorOperator) model.VectorOperator {
	if len(operators) == 1 {
		return operators[0]
	}
	oper := &coalesce{
		sampleOffsets: make([]uint64, len(operators)),
		operators:     operators,
		inVectors:     make([][]model.StepVector, len(operators)),
		batchSize:     batchSize,
	}

	return telemetry.NewOperator(telemetry.NewTelemetry(oper, opts), oper)
}

func (c *coalesce) Explain() (next []model.VectorOperator) {
	return c.operators
}

func (c *coalesce) String() string {
	return "[coalesce]"
}

func (c *coalesce) Series(ctx context.Context) ([]labels.Labels, error) {
	var err error
	c.once.Do(func() { err = c.loadSeries(ctx) })
	if err != nil {
		return nil, err
	}
	return c.series, nil
}

func (c *coalesce) Next(ctx context.Context, buf []model.StepVector) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	var err error
	c.once.Do(func() { err = c.loadSeries(ctx) })
	if err != nil {
		return 0, err
	}

	// Allocate temporary buffers on first use.
	// Inner slices will be lazily pre-allocated by child operators when they append data.
	if c.tempBufs == nil {
		c.tempBufs = make([][]model.StepVector, len(c.operators))
		for i := range c.tempBufs {
			c.tempBufs[i] = make([]model.StepVector, len(buf))
		}
	}

	var mu sync.Mutex
	var minTs int64 = math.MaxInt64
	var errChan = make(errorChan, len(c.operators))
	vectorCounts := make([]int, len(c.operators))

	for idx, o := range c.operators {
		// We already have a batch from the previous iteration.
		if c.inVectors[idx] != nil {
			mu.Lock()
			if len(c.inVectors[idx]) > 0 {
				minTs = min(minTs, c.inVectors[idx][0].T)
			}
			mu.Unlock()
			continue
		}

		c.wg.Add(1)
		go func(opIdx int, o model.VectorOperator) {
			defer c.wg.Done()

			n, err := o.Next(ctx, c.tempBufs[opIdx])
			if err != nil {
				errChan <- err
				return
			}
			vectorCounts[opIdx] = n

			// Map input IDs to output IDs.
			for i := range n {
				vector := &c.tempBufs[opIdx][i]
				for j := range vector.SampleIDs {
					vector.SampleIDs[j] = vector.SampleIDs[j] + c.sampleOffsets[opIdx]
				}
				for j := range vector.HistogramIDs {
					vector.HistogramIDs[j] = vector.HistogramIDs[j] + c.sampleOffsets[opIdx]
				}
			}

			if n > 0 {
				c.inVectors[opIdx] = c.tempBufs[opIdx][:n]
				mu.Lock()
				minTs = min(minTs, c.tempBufs[opIdx][0].T)
				mu.Unlock()
			} else {
				c.inVectors[opIdx] = nil
			}
		}(idx, o)
	}
	c.wg.Wait()
	close(errChan)

	if err := errChan.getError(); err != nil {
		return 0, err
	}

	// Count vectors with minTs and prepare output
	n := 0
	for opIdx, vectors := range c.inVectors {
		if len(vectors) == 0 || vectors[0].T != minTs {
			continue
		}

		// Initialize output vectors if needed
		if n == 0 {
			maxSteps := min(len(vectors), len(buf))
			for i := range maxSteps {
				buf[i].Reset(vectors[i].T)
				// Ensure sufficient capacity for float samples.
				// Histogram slices will grow on demand since most queries don't use them.
				totalSamples := 0
				totalHistograms := 0
				for _, v := range c.inVectors {
					if len(v) > i {
						totalSamples += len(v[i].SampleIDs)
						totalHistograms += len(v[i].HistogramIDs)
					}
				}
				if cap(buf[i].SampleIDs) < totalSamples {
					buf[i].SampleIDs = make([]uint64, 0, totalSamples)
					buf[i].Samples = make([]float64, 0, totalSamples)
				}
				if totalHistograms > 0 && cap(buf[i].HistogramIDs) < totalHistograms {
					buf[i].HistogramIDs = make([]uint64, 0, totalHistograms)
					buf[i].Histograms = make([]*histogram.FloatHistogram, 0, totalHistograms)
				}
			}
			n = maxSteps
		}

		// Append samples from this operator
		for i := 0; i < n && i < len(vectors); i++ {
			buf[i].AppendSamples(vectors[i].SampleIDs, vectors[i].Samples)
			buf[i].AppendHistograms(vectors[i].HistogramIDs, vectors[i].Histograms)
		}

		// Keep remaining vectors for next iteration
		if n < len(vectors) {
			c.inVectors[opIdx] = vectors[n:]
		} else {
			c.inVectors[opIdx] = nil
		}
	}

	return n, nil
}

func (c *coalesce) loadSeries(ctx context.Context) error {
	var wg sync.WaitGroup
	var numSeries uint64
	allSeries := make([][]labels.Labels, len(c.operators))
	errChan := make(errorChan, len(c.operators))
	for i := range c.operators {
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
	c.seriesCounts = make([]int, len(c.operators))
	c.series = make([]labels.Labels, 0, numSeries)
	for i, series := range allSeries {
		c.sampleOffsets[i] = uint64(len(c.series))
		c.seriesCounts[i] = len(series)
		c.series = append(c.series, series...)
	}

	if c.batchSize == 0 || c.batchSize > int64(len(c.series)) {
		c.batchSize = int64(len(c.series))
	}
	return nil
}
