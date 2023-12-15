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
	v1 "github.com/prometheus/prometheus/web/api/v1"
)

type remoteEngine struct {
	q         storage.Queryable
	engine    *compatibilityEngine
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

func (l remoteEngine) NewRangeQuery(ctx context.Context, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	return l.engine.NewRangeQuery(ctx, l.q, opts, qs, start, end, interval)
}

type distributedEngine struct {
	endpoints    api.RemoteEndpoints
	remoteEngine *compatibilityEngine
}

func NewDistributedEngine(opts Opts, endpoints api.RemoteEndpoints) v1.QueryEngine {
	opts.LogicalOptimizers = []logicalplan.Optimizer{
		logicalplan.PassthroughOptimizer{Endpoints: endpoints},
		logicalplan.DistributeAvgOptimizer{},
		logicalplan.DistributedExecutionOptimizer{Endpoints: endpoints},
	}

	return &distributedEngine{
		endpoints:    endpoints,
		remoteEngine: New(opts),
	}
}

func (l distributedEngine) SetQueryLogger(log promql.QueryLogger) {}

func (l distributedEngine) NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	// Truncate milliseconds to avoid mismatch in timestamps between remote and local engines.
	// Some clients might only support second precision when executing queries.
	ts = ts.Truncate(time.Second)

	return l.remoteEngine.NewInstantQuery(ctx, q, opts, qs, ts)
}

func (l distributedEngine) NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	// Truncate milliseconds to avoid mismatch in timestamps between remote and local engines.
	// Some clients might only support second precision when executing queries.
	start = start.Truncate(time.Second)
	end = end.Truncate(time.Second)
	interval = interval.Truncate(time.Second)

	return l.remoteEngine.NewRangeQuery(ctx, q, opts, qs, start, end, interval)
}
