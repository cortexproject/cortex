// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-io/promql-engine/api"
	"github.com/thanos-io/promql-engine/query"
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

func matchingEngineTime(e api.RemoteEngine, opts *query.Options) bool {
	return !(opts.Start.UnixMilli() > e.MaxT() || opts.End.UnixMilli() < e.MinT())
}

func (m PassthroughOptimizer) Optimize(plan parser.Expr, opts *query.Options) (parser.Expr, annotations.Annotations) {
	engines := m.Endpoints.Engines()
	if len(engines) == 1 {
		if !matchingEngineTime(engines[0], opts) {
			return plan, nil
		}
		return RemoteExecution{
			Engine:          engines[0],
			Query:           plan,
			QueryRangeStart: opts.Start,
		}, nil
	}

	if len(engines) == 0 {
		return plan, nil
	}

	matchingLabelsEngines := make([]api.RemoteEngine, 0, len(engines))
	TraverseBottomUp(nil, &plan, func(parent, current *parser.Expr) (stop bool) {
		if vs, ok := (*current).(*VectorSelector); ok {
			for _, e := range engines {
				if !labelSetsMatch(vs.LabelMatchers, e.LabelSets()...) {
					continue
				}

				matchingLabelsEngines = append(matchingLabelsEngines, e)
			}
		}
		return false
	})

	if len(matchingLabelsEngines) == 1 && matchingEngineTime(matchingLabelsEngines[0], opts) {
		return RemoteExecution{
			Engine:          matchingLabelsEngines[0],
			Query:           plan,
			QueryRangeStart: opts.Start,
		}, nil
	}

	return plan, nil
}
