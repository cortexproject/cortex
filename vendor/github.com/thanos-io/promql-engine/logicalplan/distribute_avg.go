// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/thanos-io/promql-engine/query"
)

// DistributeAvgOptimizer rewrites an AVG aggregation into a SUM/COUNT aggregation so that
// it can be executed in a distributed manner.
type DistributeAvgOptimizer struct {
	SkipBinaryPushdown bool
}

func (r DistributeAvgOptimizer) Optimize(plan Node, _ *query.Options) (Node, annotations.Annotations) {
	TraverseBottomUp(nil, &plan, func(parent, current *Node) (stop bool) {
		if !isDistributiveOrAverage(current, r.SkipBinaryPushdown) {
			return true
		}
		// If the current node is avg(), distribute the operation and
		// stop the traversal.
		if aggr, ok := (*current).(*Aggregation); ok {
			if aggr.Op != parser.AVG {
				return true
			}

			sum := *(*current).(*Aggregation)
			sum.Op = parser.SUM
			count := *(*current).(*Aggregation)
			count.Op = parser.COUNT
			*current = &Binary{
				Op:  parser.DIV,
				LHS: &sum,
				RHS: &count,
				VectorMatching: &parser.VectorMatching{
					Include:        aggr.Grouping,
					MatchingLabels: aggr.Grouping,
					On:             true,
				},
			}
			return true
		}
		return !isDistributiveOrAverage(parent, r.SkipBinaryPushdown)
	})
	return plan, nil
}

func isDistributiveOrAverage(expr *Node, skipBinaryPushdown bool) bool {
	if expr == nil {
		return false
	}
	var isAvg bool
	if aggr, ok := (*expr).(*Aggregation); ok {
		isAvg = aggr.Op == parser.AVG
	}
	return isDistributive(expr, skipBinaryPushdown) || isAvg
}
