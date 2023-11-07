// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package exchange

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/thanos-io/promql-engine/execution/model"

	"github.com/prometheus/prometheus/model/labels"
)

type maybeStepVector struct {
	err        error
	stepVector []model.StepVector
}

type concurrencyOperator struct {
	once       sync.Once
	next       model.VectorOperator
	buffer     chan maybeStepVector
	bufferSize int
	model.OperatorTelemetry
}

func NewConcurrent(next model.VectorOperator, bufferSize int) model.VectorOperator {
	c := &concurrencyOperator{
		next:       next,
		buffer:     make(chan maybeStepVector, bufferSize),
		bufferSize: bufferSize,
	}
	c.OperatorTelemetry = &model.TrackedTelemetry{}
	return c
}

func (c *concurrencyOperator) Analyze() (model.OperatorTelemetry, []model.ObservableVectorOperator) {
	c.SetName(("[*concurrencyOperator]"))
	next := make([]model.ObservableVectorOperator, 0, 1)
	if obsnext, ok := c.next.(model.ObservableVectorOperator); ok {
		next = append(next, obsnext)
	}
	return c, next
}

func (c *concurrencyOperator) Explain() (me string, next []model.VectorOperator) {
	return fmt.Sprintf("[*concurrencyOperator(buff=%v)]", c.bufferSize), []model.VectorOperator{c.next}
}

func (c *concurrencyOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	return c.next.Series(ctx)
}

func (c *concurrencyOperator) GetPool() *model.VectorPool {
	return c.next.GetPool()
}

func (c *concurrencyOperator) Next(ctx context.Context) ([]model.StepVector, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	start := time.Now()
	c.once.Do(func() {
		go c.pull(ctx)
		go c.drainBufferOnCancel(ctx)
	})

	r, ok := <-c.buffer
	if !ok {
		return nil, nil
	}
	if r.err != nil {
		return nil, r.err
	}
	c.AddExecutionTimeTaken(time.Since(start))

	return r.stepVector, nil
}

func (c *concurrencyOperator) pull(ctx context.Context) {
	defer close(c.buffer)

	for {
		select {
		case <-ctx.Done():
			c.buffer <- maybeStepVector{err: ctx.Err()}
			return
		default:
			r, err := c.next.Next(ctx)
			if err != nil {
				c.buffer <- maybeStepVector{err: err}
				return
			}
			if r == nil {
				return
			}
			c.buffer <- maybeStepVector{stepVector: r}
		}
	}
}

func (c *concurrencyOperator) drainBufferOnCancel(ctx context.Context) {
	<-ctx.Done()
	for range c.buffer {
	}
}
