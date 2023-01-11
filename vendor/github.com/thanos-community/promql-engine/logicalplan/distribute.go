// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"fmt"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-community/promql-engine/api"
)

type Coalesce struct {
	Expressions parser.Expressions
}

func (r Coalesce) String() string {
	return fmt.Sprintf("coalesce(%s)", r.Expressions)
}

func (r Coalesce) Pretty(level int) string { return r.String() }

func (r Coalesce) PositionRange() parser.PositionRange { return parser.PositionRange{} }

func (r Coalesce) Type() parser.ValueType { return parser.ValueTypeMatrix }

func (r Coalesce) PromQLExpr() {}

type RemoteExecution struct {
	Engine api.RemoteEngine
	Query  string
}

func (r RemoteExecution) String() string {
	return fmt.Sprintf("remote(%s)", r.Query)
}

func (r RemoteExecution) Pretty(level int) string { return r.String() }

func (r RemoteExecution) PositionRange() parser.PositionRange { return parser.PositionRange{} }

func (r RemoteExecution) Type() parser.ValueType { return parser.ValueTypeMatrix }

func (r RemoteExecution) PromQLExpr() {}

var distributiveAggregations = map[parser.ItemType]struct{}{
	parser.SUM:     {},
	parser.MIN:     {},
	parser.MAX:     {},
	parser.GROUP:   {},
	parser.COUNT:   {},
	parser.BOTTOMK: {},
	parser.TOPK:    {},
}

// DistributedExecutionOptimizer produces a logical plan suitable for
// distributed Query execution.
type DistributedExecutionOptimizer struct {
	Endpoints api.RemoteEndpoints
}

func (m DistributedExecutionOptimizer) Optimize(plan parser.Expr) parser.Expr {
	engines := m.Endpoints.Engines()
	traverseBottomUp(nil, &plan, func(parent, current *parser.Expr) (stop bool) {
		// If the current operation is not distributive, stop the traversal.
		if !isDistributive(current) {
			return true
		}

		// If the current node is an aggregation, distribute the operation and
		// stop the traversal.
		if aggr, ok := (*current).(*parser.AggregateExpr); ok {
			localAggregation := aggr.Op
			if aggr.Op == parser.COUNT {
				localAggregation = parser.SUM
			}
			subQueries := m.makeSubQueries(current, engines)
			*current = &parser.AggregateExpr{
				Op:       localAggregation,
				Expr:     subQueries,
				Param:    aggr.Param,
				Grouping: aggr.Grouping,
				Without:  aggr.Without,
				PosRange: aggr.PosRange,
			}
			return true
		}

		// If the parent operation is distributive, continue the traversal.
		if isDistributive(parent) {
			return false
		}

		*current = m.makeSubQueries(current, engines)
		return true
	})

	return plan
}

func (m DistributedExecutionOptimizer) makeSubQueries(current *parser.Expr, engines []api.RemoteEngine) Coalesce {
	remoteQueries := Coalesce{
		Expressions: make(parser.Expressions, len(engines)),
	}
	for i := 0; i < len(engines); i++ {
		remoteQueries.Expressions[i] = &RemoteExecution{
			Engine: engines[i],
			Query:  (*current).String(),
		}
	}
	return remoteQueries
}

func isDistributive(expr *parser.Expr) bool {
	if expr == nil {
		return false
	}
	switch aggr := (*expr).(type) {
	case *parser.BinaryExpr:
		// Binary expressions are joins and need to be done across the entire
		// data set. This is why we cannot push down aggregations where
		// the operand is a binary expression.
		return false
	case *parser.AggregateExpr:
		// Certain aggregations are currently not supported.
		if _, ok := distributiveAggregations[aggr.Op]; !ok {
			return false
		}
	}

	return true
}
