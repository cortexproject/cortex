// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package api

import (
	"time"

	"github.com/prometheus/prometheus/promql"
)

type RemoteEndpoints interface {
	Engines() []RemoteEngine
}

type RemoteEngine interface {
	NewInstantQuery(opts *promql.QueryOpts, qs string, ts time.Time) (promql.Query, error)
	NewRangeQuery(opts *promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error)
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
