// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"maps"
	"slices"

	"github.com/thanos-io/promql-engine/api"
	"github.com/thanos-io/promql-engine/query"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/annotations"
)

// PassthroughOptimizer optimizes queries which can be simply passed
// through to a RemoteEngine.
type PassthroughOptimizer struct {
	Endpoints api.RemoteEndpoints
}

// labelSetsMatch returns false if all label-set do not match the matchers (aka: OR is between all label-sets).
func labelSetsMatch(matchers []*labels.Matcher, lset ...labels.Labels) bool {
	if len(lset) == 0 {
		return true
	}

	for _, ls := range lset {
		notMatched := false
		for _, m := range matchers {
			if lv := ls.Get(m.Name); ls.Has(m.Name) && !m.Matches(lv) {
				notMatched = true
				break
			}
		}
		if !notMatched {
			return true
		}
	}
	return false
}

func (m PassthroughOptimizer) Optimize(plan Node, opts *query.Options) (Node, annotations.Annotations) {
	mint, maxt := MinMaxTime(plan, opts)
	engines := m.Endpoints.Engines(mint, maxt)
	if len(engines) == 0 {
		return plan, nil
	}
	var (
		hasSelector       bool
		matchingEngineSet = make(map[api.RemoteEngine]struct{})
	)
	TraverseBottomUp(nil, &plan, func(parent, current *Node) (stop bool) {
		if vs, ok := (*current).(*VectorSelector); ok {
			hasSelector = true
			for _, e := range engines {
				if !labelSetsMatch(vs.LabelMatchers, e.LabelSets()...) {
					continue
				}
				matchingEngineSet[e] = struct{}{}
				if len(matchingEngineSet) > 1 {
					return true
				}
			}
		}
		return false
	})

	matchingEngines := slices.Collect(maps.Keys(matchingEngineSet))
	if len(matchingEngines) == 0 {
		if !hasSelector && matchingEngineTime(engines[0], mint, maxt) {
			return RemoteExecution{
				Engine:          engines[0],
				Query:           plan.Clone(),
				QueryRangeStart: opts.Start,
				QueryRangeEnd:   opts.End,
			}, nil
		}
		return plan, nil
	}

	if len(matchingEngines) == 1 && matchingEngineTime(matchingEngines[0], mint, maxt) {
		return RemoteExecution{
			Engine:          matchingEngines[0],
			Query:           plan.Clone(),
			QueryRangeStart: opts.Start,
			QueryRangeEnd:   opts.End,
		}, nil
	}

	return plan, nil
}

func matchingEngineTime(e api.RemoteEngine, minTime, maxTime int64) bool {
	return !(minTime > e.MaxT() || maxTime < e.MinT())
}
