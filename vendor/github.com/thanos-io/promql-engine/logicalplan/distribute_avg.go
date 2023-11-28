package logicalplan

import (
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/thanos-io/promql-engine/query"
)

// DistributeAvgOptimizer rewrites an AVG aggregation into a SUM/COUNT aggregation so that
// it can be executed in a distributed manner.
type DistributeAvgOptimizer struct{}

func (r DistributeAvgOptimizer) Optimize(plan parser.Expr, _ *query.Options) parser.Expr {
	TraverseBottomUp(nil, &plan, func(parent, current *parser.Expr) (stop bool) {
		// If the current operation is not distributive, stop the traversal.
		if !isDistributive(current) {
			return true
		}

		// If the current node is avg(), distribute the operation and
		// stop the traversal.
		if aggr, ok := (*current).(*parser.AggregateExpr); ok {
			if aggr.Op != parser.AVG {
				return true
			}

			sum := *(*current).(*parser.AggregateExpr)
			sum.Op = parser.SUM
			count := *(*current).(*parser.AggregateExpr)
			count.Op = parser.COUNT
			*current = &parser.BinaryExpr{
				Op:             parser.DIV,
				LHS:            &sum,
				RHS:            &count,
				VectorMatching: &parser.VectorMatching{},
			}
			return true
		}

		// If the parent operation is distributive, continue the traversal.
		return !isDistributive(parent)
	})
	return plan
}
