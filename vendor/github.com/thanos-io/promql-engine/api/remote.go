// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package api

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

type RemoteEndpoints interface {
	Engines() []RemoteEngine
}

type RemoteEngine interface {
	MaxT() int64
	MinT() int64
	LabelSets() []labels.Labels
	NewRangeQuery(ctx context.Context, opts promql.QueryOpts, plan parser.Expr, start, end time.Time, interval time.Duration) (promql.Query, error)
}

type staticEndpoints struct {
	engines []RemoteEngine
}

func (m staticEndpoints) Engines() []RemoteEngine {
	return m.engines
}

func NewStaticEndpoints(engines []RemoteEngine) RemoteEndpoints {
	return &staticEndpoints{engines: engines}
}
