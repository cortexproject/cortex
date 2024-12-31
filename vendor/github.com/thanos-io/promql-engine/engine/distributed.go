// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package engine

import (
	"context"
	"time"

	"github.com/thanos-io/promql-engine/api"
	"github.com/thanos-io/promql-engine/logicalplan"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
)

type remoteEngine struct {
	q         storage.Queryable
	engine    *Engine
	labelSets []labels.Labels
	maxt      int64
	mint      int64
}

func NewRemoteEngine(opts Opts, q storage.Queryable, mint, maxt int64, labelSets []labels.Labels) *remoteEngine {
	return &remoteEngine{
		q:         q,
		labelSets: labelSets,
		maxt:      maxt,
		mint:      mint,
		engine:    New(opts),
	}
}

func (l remoteEngine) MaxT() int64 {
	return l.maxt
}

func (l remoteEngine) MinT() int64 {
	return l.mint
}

func (l remoteEngine) LabelSets() []labels.Labels {
	return l.labelSets
}

func (l remoteEngine) NewRangeQuery(ctx context.Context, opts promql.QueryOpts, plan api.RemoteQuery, start, end time.Time, interval time.Duration) (promql.Query, error) {
	return l.engine.NewRangeQuery(ctx, l.q, opts, plan.String(), start, end, interval)
}

type DistributedEngine struct {
	endpoints    api.RemoteEndpoints
	remoteEngine *Engine
}

func NewDistributedEngine(opts Opts, endpoints api.RemoteEndpoints) *DistributedEngine {
	opts.LogicalOptimizers = []logicalplan.Optimizer{
		logicalplan.PassthroughOptimizer{Endpoints: endpoints},
		logicalplan.DistributedExecutionOptimizer{Endpoints: endpoints},
	}

	return &DistributedEngine{
		endpoints:    endpoints,
		remoteEngine: New(opts),
	}
}

func (l DistributedEngine) SetQueryLogger(log promql.QueryLogger) {}

func (l DistributedEngine) NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	return l.MakeInstantQuery(ctx, q, fromPromQLOpts(opts), qs, ts)
}

func (l DistributedEngine) NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	return l.MakeRangeQuery(ctx, q, fromPromQLOpts(opts), qs, start, end, interval)
}

func (l DistributedEngine) MakeInstantQueryFromPlan(ctx context.Context, q storage.Queryable, opts *QueryOpts, plan logicalplan.Node, ts time.Time) (promql.Query, error) {
	// Truncate milliseconds to avoid mismatch in timestamps between remote and local engines.
	// Some clients might only support second precision when executing queries.
	ts = ts.Truncate(time.Second)

	return l.remoteEngine.MakeInstantQueryFromPlan(ctx, q, opts, plan, ts)
}

func (l DistributedEngine) MakeRangeQueryFromPlan(ctx context.Context, q storage.Queryable, opts *QueryOpts, plan logicalplan.Node, start, end time.Time, interval time.Duration) (promql.Query, error) {
	// Truncate milliseconds to avoid mismatch in timestamps between remote and local engines.
	// Some clients might only support second precision when executing queries.
	start = start.Truncate(time.Second)
	end = end.Truncate(time.Second)
	interval = interval.Truncate(time.Second)

	return l.remoteEngine.MakeRangeQueryFromPlan(ctx, q, opts, plan, start, end, interval)
}

func (l DistributedEngine) MakeInstantQuery(ctx context.Context, q storage.Queryable, opts *QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	// Truncate milliseconds to avoid mismatch in timestamps between remote and local engines.
	// Some clients might only support second precision when executing queries.
	ts = ts.Truncate(time.Second)

	return l.remoteEngine.MakeInstantQuery(ctx, q, opts, qs, ts)
}

func (l DistributedEngine) MakeRangeQuery(ctx context.Context, q storage.Queryable, opts *QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	// Truncate milliseconds to avoid mismatch in timestamps between remote and local engines.
	// Some clients might only support second precision when executing queries.
	start = start.Truncate(time.Second)
	end = end.Truncate(time.Second)
	interval = interval.Truncate(time.Second)

	return l.remoteEngine.MakeRangeQuery(ctx, q, opts, qs, start, end, interval)
}
