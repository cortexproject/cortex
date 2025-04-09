// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package noop

import (
	"context"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/query"
	"github.com/thanos-io/promql-engine/storage/prometheus"

	"github.com/prometheus/prometheus/model/labels"
)

type operator struct {
	model.VectorOperator
}

func NewOperator(opts *query.Options) model.VectorOperator {
	scanner := prometheus.NewVectorSelector(
		model.NewVectorPool(0),
		noopSelector{},
		opts,
		0, 0, false, 0, 1,
	)
	return &operator{VectorOperator: scanner}
}

func (o operator) String() string                         { return "[noop]" }
func (o operator) Explain() (next []model.VectorOperator) { return nil }

type noopSelector struct{}

func (n noopSelector) Matchers() []*labels.Matcher { return nil }
func (n noopSelector) GetSeries(ctx context.Context, shard, numShards int) ([]prometheus.SignedSeries, error) {
	return nil, nil
}
