package astmapper

import (
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql"
)

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
		// currently we only support sum for expediency.
		if n.Op != promql.ItemSum || !CanParallel(n.Param) || !CanParallel(n.Expr) {
			return false
		}
		return true

	case *promql.BinaryExpr:
		// since binary exprs use each side for merging, they cannot be parallelized
		return false

	case *promql.Call:
		if !funcParallel(n.Func) {
			return false
		}

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

func funcParallel(f *promql.Function) bool {
	// flagging all functions as parallel for expediency -- this is certainly not correct (i.e. avg).
	return true
}
