// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/promql/parser"
)

// UnwrapString recursively unwraps a parser.Expr until it reaches an StringLiteral.
func UnwrapString(expr Node) (string, error) {
	switch texpr := expr.(type) {
	case *StringLiteral:
		return texpr.Val, nil
	case *Parens:
		return UnwrapString(texpr.Expr)
	case *StepInvariantExpr:
		return UnwrapString(texpr.Expr)
	default:
		return "", errors.Newf("unexpected type: %T", texpr)
	}
}

// UnsafeUnwrapString is like UnwrapString but should only be used in cases where the parser
// guarantees success by already only allowing strings wrapped in parentheses.
func UnsafeUnwrapString(expr Node) string {
	v, _ := UnwrapString(expr)
	return v
}

// UnwrapFloat recursively unwraps a parser.Expr until it reaches an NumberLiteral.
func UnwrapFloat(expr Node) (float64, error) {
	switch texpr := expr.(type) {
	case *NumberLiteral:
		return texpr.Val, nil
	case *Parens:
		return UnwrapFloat(texpr.Expr)
	case *StepInvariantExpr:
		return UnwrapFloat(texpr.Expr)
	default:
		return 0, errors.Newf("unexpected type: %T", texpr)
	}
}

// UnwrapParens recursively unwraps a parser.ParenExpr.
func UnwrapParens(expr parser.Expr) parser.Expr {
	switch t := expr.(type) {
	case *parser.ParenExpr:
		return UnwrapParens(t.Expr)
	default:
		return t
	}
}

// IsConstantExpr reports if the expression evaluates to a constant.
func IsConstantExpr(expr Node) bool {
	// TODO: there are more possibilities for constant expressions
	switch texpr := expr.(type) {
	case *NumberLiteral, *StringLiteral:
		return true
	case *StepInvariantExpr:
		return IsConstantExpr(texpr.Expr)
	case *Parens:
		return IsConstantExpr(texpr.Expr)
	case *FunctionCall:
		constArgs := true
		for _, arg := range texpr.Args {
			constArgs = constArgs && IsConstantExpr(arg)
		}
		return constArgs
	case *Binary:
		return IsConstantExpr(texpr.LHS) && IsConstantExpr(texpr.RHS)
	default:
		return false
	}
}

// IsConstantScalarExpr reports if the expression evaluates to a scalar.
func IsConstantScalarExpr(expr Node) bool {
	// TODO: there are more possibilities for constant expressions
	switch texpr := expr.(type) {
	case *NumberLiteral, *StringLiteral:
		return true
	case *StepInvariantExpr:
		return IsConstantScalarExpr(texpr.Expr)
	case *Parens:
		return IsConstantScalarExpr(texpr.Expr)
	case *Binary:
		return IsConstantScalarExpr(texpr.LHS) && IsConstantScalarExpr(texpr.RHS)
	default:
		return false
	}
}
