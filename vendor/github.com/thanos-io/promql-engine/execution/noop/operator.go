// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package noop

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/promql-engine/execution/model"
)

type operator struct {
	// this needs a pool so we dont panic if we want use "GetPool().PutVectors"
	pool *model.VectorPool
}

func NewOperator() model.VectorOperator { return &operator{pool: model.NewVectorPool(0)} }

func (o operator) String() string { return "[noop]" }

func (o operator) Next(ctx context.Context) ([]model.StepVector, error) { return nil, nil }

func (o operator) Series(ctx context.Context) ([]labels.Labels, error) { return nil, nil }

func (o operator) GetPool() *model.VectorPool { return o.pool }

func (o operator) Explain() (next []model.VectorOperator) { return nil }
