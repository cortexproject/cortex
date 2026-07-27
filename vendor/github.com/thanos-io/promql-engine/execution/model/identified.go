// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package model

import (
	"context"
	"sync"

	"github.com/prometheus/prometheus/model/labels"
)

// identifiedOperator wraps a VectorOperator with a deterministic fingerprint.
type identifiedOperator struct {
	VectorOperator
	id          uint64
	enrichedCtx context.Context
	once        sync.Once
}

// WithID wraps op so that it implements OperatorIDer.
func WithID(op VectorOperator, id uint64) VectorOperator {
	return &identifiedOperator{VectorOperator: op, id: id}
}

func (o *identifiedOperator) OperatorID() uint64 { return o.id }

func (o *identifiedOperator) enriched(ctx context.Context) context.Context {
	o.once.Do(func() { o.enrichedCtx = ContextWithOperatorID(ctx, o.id) })
	return o.enrichedCtx
}

func (o *identifiedOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	return o.VectorOperator.Series(o.enriched(ctx))
}

func (o *identifiedOperator) Next(ctx context.Context, buf []StepVector) (int, error) {
	return o.VectorOperator.Next(o.enriched(ctx), buf)
}

func (o *identifiedOperator) Unwrap() VectorOperator { return o.VectorOperator }

type Unwrapper interface {
	Unwrap() VectorOperator
}

func Unwrap(op VectorOperator) VectorOperator {
	for {
		u, ok := op.(Unwrapper)
		if !ok {
			return op
		}
		op = u.Unwrap()
	}
}
