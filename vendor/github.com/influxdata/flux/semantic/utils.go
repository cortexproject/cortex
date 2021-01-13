package semantic

import "github.com/influxdata/flux/ast"

// ConjunctionsToExprSlice finds all children of AndOperators that are not themselves AndOperators,
// and returns them in a slice.  If the root node of expr is not an AndOperator, just returns expr.
//
//      AND
//     /   \
//    AND   r    =>   {p, q, r}
//   /   \
//  p     q
//
func ConjunctionsToExprSlice(expr Expression) []Expression {
	if e, ok := expr.(*LogicalExpression); ok && e.Operator == ast.AndOperator {
		exprSlice := make([]Expression, 0, 2)
		exprSlice = append(exprSlice, ConjunctionsToExprSlice(e.Left)...)
		exprSlice = append(exprSlice, ConjunctionsToExprSlice(e.Right)...)
		return exprSlice
	}

	return []Expression{expr}
}

// ExprsToConjunction accepts a variable number of expressions and ANDs them
// together into a single expression.
//
//                         AND
//                        /   \
//    {p, q, r}    =>    AND   r
//                      /   \
//                     p     q
//
func ExprsToConjunction(exprs ...Expression) Expression {
	if len(exprs) == 0 {
		return nil
	}

	expr := exprs[0]
	for _, e := range exprs[1:] {
		expr = &LogicalExpression{
			Left:     expr,
			Right:    e,
			Operator: ast.AndOperator,
		}
	}

	return expr
}

// PartitionPredicates accepts a predicate expression, separates it into components that have been
// logically ANDed together, and applies partitionFn to them.  Returns two expressions: one AND tree
// of the expressions for which partitionFn returned true, and an AND tree of expressions for which
// partitionFn returned false.
//
// Suppose partitonFn returns true for p and r, and false for q:
//
//      AND           passExpr     failExpr
//     /   \
//    AND   r    =>     AND           q
//   /   \             /   \
//  p     q           p     r
//
func PartitionPredicates(expr Expression, partitionFn func(expression Expression) (bool, error)) (passExpr, failExpr Expression, err error) {
	exprSlice := ConjunctionsToExprSlice(expr)
	var passSlice, failSlice []Expression
	for _, e := range exprSlice {
		b, err := partitionFn(e)
		if err != nil {
			return nil, nil, err
		}
		if b {
			passSlice = append(passSlice, e)
		} else {
			failSlice = append(failSlice, e)
		}
	}

	passExpr = ExprsToConjunction(passSlice...)
	failExpr = ExprsToConjunction(failSlice...)
	return passExpr, failExpr, nil
}
