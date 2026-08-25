// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package api

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
)

type RemoteQuery interface {
	fmt.Stringer
}

// RemoteEndpoints returns remote engines.
//
// Implementations should use mint and maxt to prune engine metadata
// (e.g., filter TSDBInfos to only those overlapping the time range),
// reducing unnecessary computations in subsequent calls to methods like
// RemoteEngine.LabelSets().
//
// All available engines should be returned regardless of pruning.
type RemoteEndpoints interface {
	// Engines returns remote engines.
	//
	// If mint and/or maxt of the query is unknown, the caller must pass
	// math.MinInt64 and math.MaxInt64 respectively to retrieve unpruned engines.
	Engines(mint, maxt int64) []RemoteEngine
}

type RemoteEngine interface {
	MaxT() int64
	MinT() int64

	// The external labels of the remote engine. These are used to limit fanout. The engine uses these to
	// not distribute into remote engines that would return empty responses because their labelset is not matching.
	LabelSets() []labels.Labels

	// The external labels of the remote engine that form a logical partition. This is expected to be
	// a subset of the result of "LabelSets()". The engine uses these to compute how to distribute a query.
	// It is important that, for a given set of remote engines, these labels do not overlap meaningfully.
	PartitionLabelSets() []labels.Labels

	NewRangeQuery(ctx context.Context, opts promql.QueryOpts, plan RemoteQuery, start, end time.Time, interval time.Duration) (promql.Query, error)
}

type staticEndpoints struct {
	engines []RemoteEngine
}

func (m staticEndpoints) Engines(mint, maxt int64) []RemoteEngine {
	return m.engines
}

func NewStaticEndpoints(engines []RemoteEngine) RemoteEndpoints {
	return &staticEndpoints{engines: engines}
}

type cachedEndpoints struct {
	endpoints RemoteEndpoints

	enginesOnce sync.Once
	engines     []RemoteEngine
}

func (l *cachedEndpoints) Engines(mint, maxt int64) []RemoteEngine {
	l.enginesOnce.Do(func() {
		l.engines = l.endpoints.Engines(mint, maxt)
	})
	return l.engines
}

// NewCachedEndpoints returns an endpoints wrapper that
// resolves and caches engines on first access.
//
// All subsequent Engines calls return cached engines, ignoring any query
// parameters.
func NewCachedEndpoints(endpoints RemoteEndpoints) RemoteEndpoints {
	if endpoints == nil {
		panic("api.NewCachedEndpoints: endpoints is nil")
	}

	if le, ok := endpoints.(*cachedEndpoints); ok {
		return le
	}

	return &cachedEndpoints{endpoints: endpoints}
}
