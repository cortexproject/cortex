// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package exchange

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/query"

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
	telemetry.OperatorTelemetry
}

func NewConcurrent(next model.VectorOperator, bufferSize int, opts *query.Options) model.VectorOperator {
	oper := &concurrencyOperator{
		next:       next,
		buffer:     make(chan maybeStepVector, bufferSize),
		bufferSize: bufferSize,
	}

	oper.OperatorTelemetry = telemetry.NewTelemetry(oper, opts)
	return oper
}

func (c *concurrencyOperator) Explain() (next []model.VectorOperator) {
	return []model.VectorOperator{c.next}
}

func (c *concurrencyOperator) String() string {
	return fmt.Sprintf("[concurrent(buff=%v)]", c.bufferSize)
}

func (c *concurrencyOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	start := time.Now()
	defer func() { c.AddExecutionTimeTaken(time.Since(start)) }()

	return c.next.Series(ctx)
}

func (c *concurrencyOperator) GetPool() *model.VectorPool {
	return c.next.GetPool()
}

func (c *concurrencyOperator) Next(ctx context.Context) ([]model.StepVector, error) {
	start := time.Now()
	defer func() { c.AddExecutionTimeTaken(time.Since(start)) }()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

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
