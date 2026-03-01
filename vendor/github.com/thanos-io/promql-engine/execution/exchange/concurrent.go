// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package exchange

import (
	"context"
	"fmt"
	"sync"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/query"

	"github.com/prometheus/prometheus/model/labels"
)

type maybeStepVector struct {
	err     error
	vectors []model.StepVector // The actual buffer with data
	n       int
}

type concurrencyOperator struct {
	once       sync.Once
	seriesOnce sync.Once
	next       model.VectorOperator
	buffer     chan maybeStepVector
	bufferSize int
	opts       *query.Options

	// Buffer management for zero-copy swapping
	// We maintain a pool of buffers that get swapped between producer and consumer
	returnChan chan []model.StepVector // Channel to return buffers for reuse

	// seriesCount is used to pre-allocate inner slices of StepVectors
	seriesCount int
}

func NewConcurrent(next model.VectorOperator, bufferSize int, opts *query.Options) model.VectorOperator {
	oper := &concurrencyOperator{
		next:       next,
		buffer:     make(chan maybeStepVector, bufferSize),
		bufferSize: bufferSize,
		opts:       opts,
		returnChan: make(chan []model.StepVector, bufferSize+2),
	}

	return telemetry.NewOperator(telemetry.NewTelemetry(oper, opts), oper)
}

func (c *concurrencyOperator) Explain() (next []model.VectorOperator) {
	return []model.VectorOperator{c.next}
}

func (c *concurrencyOperator) String() string {
	return fmt.Sprintf("[concurrent(buff=%v)]", c.bufferSize)
}

func (c *concurrencyOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	series, err := c.next.Series(ctx)
	if err != nil {
		return nil, err
	}

	// Initialize buffers. Inner slices will be allocated by the child operator
	// which knows the actual batch size for pre-allocation.
	c.seriesOnce.Do(func() {
		c.seriesCount = len(series)
		for i := 0; i < c.bufferSize+1; i++ {
			c.returnChan <- make([]model.StepVector, c.opts.StepsBatch)
		}
	})

	return series, nil
}

func (c *concurrencyOperator) Next(ctx context.Context, buf []model.StepVector) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	// Ensure buffers are initialized (in case Series() wasn't called first)
	c.seriesOnce.Do(func() {
		// Fallback: create buffers without pre-sized inner slices
		for i := 0; i < c.bufferSize+1; i++ {
			c.returnChan <- make([]model.StepVector, c.opts.StepsBatch)
		}
	})

	c.once.Do(func() {
		go c.pull(ctx)
		go c.drainBufferOnCancel(ctx)
	})

	r, ok := <-c.buffer
	if !ok {
		return 0, nil
	}
	if r.err != nil {
		return 0, r.err
	}

	// Zero-copy swap: move data from internal buffer to caller's buffer
	// by swapping the slice contents directly
	n := min(r.n, len(buf))
	for i := range n {
		// Swap the step vector contents (this is just pointer/slice header swaps, not data copy)
		buf[i], r.vectors[i] = r.vectors[i], buf[i]
	}

	// Return the (now empty) buffer for reuse by the producer
	c.returnChan <- r.vectors

	return n, nil
}

func (c *concurrencyOperator) pull(ctx context.Context) {
	defer close(c.buffer)

	for {
		select {
		case <-ctx.Done():
			c.buffer <- maybeStepVector{err: ctx.Err()}
			return
		default:
			// Get an available buffer from the return channel
			var readBuf []model.StepVector
			select {
			case readBuf = <-c.returnChan:
			case <-ctx.Done():
				c.buffer <- maybeStepVector{err: ctx.Err()}
				return
			}

			n, err := c.next.Next(ctx, readBuf)
			if err != nil {
				// Return the buffer
				c.returnChan <- readBuf
				c.buffer <- maybeStepVector{err: err}
				return
			}
			if n == 0 {
				// Return the buffer
				c.returnChan <- readBuf
				return
			}

			// Send the buffer with data
			c.buffer <- maybeStepVector{vectors: readBuf, n: n}
		}
	}
}

func (c *concurrencyOperator) drainBufferOnCancel(ctx context.Context) {
	<-ctx.Done()
	for r := range c.buffer {
		if r.vectors != nil {
			// Return the buffer
			c.returnChan <- r.vectors
		}
	}
}
