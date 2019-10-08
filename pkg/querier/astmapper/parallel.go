package astmapper

import (
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql"
)

// matrixselector & vectorselector need to have shard annotations added
/*
Design:
1) annotate subtrees as parallelizable. A subtree is parallelizable if all of it's components are parallelizable
2) deal with splitting/stiching against queriers after mapping into the parallel-compatible AST
*/

// function to annotate subtrees as parallelizable. Inefficient, but illustrative.
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
		} else {
			return true
		}

	case *promql.BinaryExpr:
		// since binary exprs use each side for mapping, they cannot be parallelized
		return false

	case *promql.Call:
		if !FuncParallel(n.Func) {
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
		// cant' find an example for this -- defaulting to using the subexpr
		return CanParallel(n.Expr)

	case *promql.MatrixSelector, *promql.NumberLiteral, *promql.StringLiteral, *promql.VectorSelector:
		return true

	default:
		panic(errors.Errorf("CloneNode: unhandled node type %T", node))
	}

	return false
}

func FuncParallel(f *promql.Function) bool {
	// flagging all functions as parallel for expediency -- this is certainly not correct (i.e. avg).
	return true
}
