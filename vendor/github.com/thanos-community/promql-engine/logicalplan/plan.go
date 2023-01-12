// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

var (
	NoOptimizers  = []Optimizer{}
	AllOptimizers = append(DefaultOptimizers, PropagateMatchersOptimizer{})
)

var DefaultOptimizers = []Optimizer{
	SortMatchers{},
	MergeSelectsOptimizer{},
}

type Plan interface {
	Optimize([]Optimizer) Plan
	Expr() parser.Expr
}

type Optimizer interface {
	Optimize(parser.Expr) parser.Expr
}

type plan struct {
	expr parser.Expr
}

func New(expr parser.Expr, mint, maxt time.Time) Plan {
	expr = promql.PreprocessExpr(expr, mint, maxt)
	setOffsetForAtModifier(mint.UnixMilli(), expr)

	return &plan{
		expr: expr,
	}
}

func (p *plan) Optimize(optimizers []Optimizer) Plan {
	for _, o := range optimizers {
		p.expr = o.Optimize(p.expr)
	}

	return &plan{p.expr}
}

func (p *plan) Expr() parser.Expr {
	return p.expr
}

func traverse(expr *parser.Expr, transform func(*parser.Expr)) {
	switch node := (*expr).(type) {
	case *parser.StepInvariantExpr:
		transform(&node.Expr)
	case *parser.VectorSelector:
		transform(expr)
	case *parser.MatrixSelector:
		transform(&node.VectorSelector)
	case *parser.AggregateExpr:
		transform(expr)
		traverse(&node.Expr, transform)
	case *parser.Call:
		for _, n := range node.Args {
			traverse(&n, transform)
		}
	case *parser.BinaryExpr:
		transform(expr)
		traverse(&node.LHS, transform)
		traverse(&node.RHS, transform)
	case *parser.UnaryExpr:
		traverse(&node.Expr, transform)
	case *parser.ParenExpr:
		traverse(&node.Expr, transform)
	case *parser.SubqueryExpr:
		traverse(&node.Expr, transform)
	}
}

func traverseBottomUp(parent *parser.Expr, current *parser.Expr, transform func(parent *parser.Expr, node *parser.Expr) bool) bool {
	switch node := (*current).(type) {
	case *parser.StepInvariantExpr:
		return traverseBottomUp(current, &node.Expr, transform)
	case *parser.VectorSelector:
		return transform(parent, current)
	case *parser.MatrixSelector:
		return transform(parent, &node.VectorSelector)
	case *parser.AggregateExpr:
		if stop := traverseBottomUp(current, &node.Expr, transform); stop {
			return stop
		}
		return transform(parent, current)
	case *parser.Call:
		for _, n := range node.Args {
			if stop := traverseBottomUp(current, &n, transform); stop {
				return stop
			}
		}
		return transform(parent, current)
	case *parser.BinaryExpr:
		lstop := traverseBottomUp(current, &node.LHS, transform)
		rstop := traverseBottomUp(current, &node.RHS, transform)
		if lstop || rstop {
			return true
		}
		return transform(parent, current)
	case *parser.UnaryExpr:
		return traverseBottomUp(current, &node.Expr, transform)
	case *parser.ParenExpr:
		return traverseBottomUp(current, &node.Expr, transform)
	case *parser.SubqueryExpr:
		return traverseBottomUp(current, &node.Expr, transform)
	}

	return true
}

// Copy from https://github.com/prometheus/prometheus/blob/v2.39.1/promql/engine.go#L2658.
func setOffsetForAtModifier(evalTime int64, expr parser.Expr) {
	getOffset := func(ts *int64, originalOffset time.Duration, path []parser.Node) time.Duration {
		if ts == nil {
			return originalOffset
		}
		// TODO: support subquery.

		offsetForTs := time.Duration(evalTime-*ts) * time.Millisecond
		offsetDiff := offsetForTs
		return originalOffset + offsetDiff
	}

	parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
		switch n := node.(type) {
		case *parser.VectorSelector:
			n.Offset = getOffset(n.Timestamp, n.OriginalOffset, path)

		case *parser.MatrixSelector:
			vs := n.VectorSelector.(*parser.VectorSelector)
			vs.Offset = getOffset(vs.Timestamp, vs.OriginalOffset, path)

		case *parser.SubqueryExpr:
			n.Offset = getOffset(n.Timestamp, n.OriginalOffset, path)
		}
		return nil
	})
}
