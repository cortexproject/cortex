package astmapper

import (
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql"
)

var summableAggregates = map[promql.ItemType]struct{}{
	promql.ItemSum:     {},
	promql.ItemMin:     {},
	promql.ItemMax:     {},
	promql.ItemTopK:    {},
	promql.ItemBottomK: {},
	promql.ItemCount:   {},
}

// CanParallel annotates subtrees as parallelizable.
// A subtree is parallelizable if all of it's components are parallelizable.
func CanParallel(node promql.Node) bool {
	switch n := node.(type) {
	case nil:
		// nil handles cases where we check optional fields that are not set
		return true

	case promql.Expressions:
		for _, e := range n {
			if !CanParallel(e) {
				return false
			}
		}
		return true

	case *promql.AggregateExpr:
		_, ok := summableAggregates[n.Op]
		return ok && CanParallel(n.Expr)

	case *promql.BinaryExpr:
		// since binary exprs use each side for merging, they cannot be parallelized
		return false

	case *promql.Call:
		for _, e := range n.Args {
			if !CanParallel(e) {
				return false
			}
		}
		return true

	case *promql.SubqueryExpr:
		return CanParallel(n.Expr)

	case *promql.ParenExpr:
		return CanParallel(n.Expr)

	case *promql.UnaryExpr:
		// Since these are only currently supported for Scalars, should be parallel-compatible
		return true

	case *promql.EvalStmt:
		return CanParallel(n.Expr)

	case *promql.MatrixSelector, *promql.NumberLiteral, *promql.StringLiteral, *promql.VectorSelector:
		return true

	default:
		panic(errors.Errorf("CanParallel: unhandled node type %T", node))
	}

}
