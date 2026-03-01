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
		noopSelector{},
		opts,
		0,     // offset
		0,     // batchSize
		false, // selectTimestamp
		0,     // shard
		1,     // numShards
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
