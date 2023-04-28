// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"

	"github.com/thanos-community/promql-engine/parser"
)

var (
	NoOptimizers  = []Optimizer{}
	AllOptimizers = append(DefaultOptimizers, PropagateMatchersOptimizer{})
)

var DefaultOptimizers = []Optimizer{
	SortMatchers{},
	MergeSelectsOptimizer{},
}

type Opts struct {
	Start         time.Time
	End           time.Time
	Step          time.Duration
	LookbackDelta time.Duration
}

type Plan interface {
	Optimize([]Optimizer) Plan
	Expr() parser.Expr
}

type Optimizer interface {
	Optimize(expr parser.Expr, opts *Opts) parser.Expr
}

type plan struct {
	expr parser.Expr
	opts *Opts
}

func New(expr parser.Expr, opts *Opts) Plan {
	expr = preprocessExpr(expr, opts.Start, opts.End)
	setOffsetForAtModifier(opts.Start.UnixMilli(), expr)

	return &plan{
		expr: expr,
		opts: opts,
	}
}

func (p *plan) Optimize(optimizers []Optimizer) Plan {
	for _, o := range optimizers {
		p.expr = o.Optimize(p.expr, p.opts)
	}

	return &plan{expr: p.expr, opts: p.opts}
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
	case *parser.NumberLiteral:
		return false
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
		for i := range node.Args {
			if stop := traverseBottomUp(current, &node.Args[i], transform); stop {
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

// preprocessExpr wraps all possible step invariant parts of the given expression with
// StepInvariantExpr. It also resolves the preprocessors.
// Copied from Prometheus and adjusted to work with the vendored parser:
// https://github.com/prometheus/prometheus/blob/3ac49d4ae210869043e6c33e3a82f13f2f849361/promql/engine.go#L2676-L2684
func preprocessExpr(expr parser.Expr, start, end time.Time) parser.Expr {
	isStepInvariant := preprocessExprHelper(expr, start, end)
	if isStepInvariant {
		return newStepInvariantExpr(expr)
	}
	return expr
}

// preprocessExprHelper wraps the child nodes of the expression
// with a StepInvariantExpr wherever it's step invariant. The returned boolean is true if the
// passed expression qualifies to be wrapped by StepInvariantExpr.
// It also resolves the preprocessors.
func preprocessExprHelper(expr parser.Expr, start, end time.Time) bool {
	switch n := expr.(type) {
	case *parser.VectorSelector:
		if n.StartOrEnd == parser.START {
			n.Timestamp = makeInt64Pointer(timestamp.FromTime(start))
		} else if n.StartOrEnd == parser.END {
			n.Timestamp = makeInt64Pointer(timestamp.FromTime(end))
		}
		return n.Timestamp != nil

	case *parser.AggregateExpr:
		return preprocessExprHelper(n.Expr, start, end)

	case *parser.BinaryExpr:
		isInvariant1, isInvariant2 := preprocessExprHelper(n.LHS, start, end), preprocessExprHelper(n.RHS, start, end)
		if isInvariant1 && isInvariant2 {
			return true
		}

		if isInvariant1 {
			n.LHS = newStepInvariantExpr(n.LHS)
		}
		if isInvariant2 {
			n.RHS = newStepInvariantExpr(n.RHS)
		}

		return false

	case *parser.Call:
		_, ok := promql.AtModifierUnsafeFunctions[n.Func.Name]
		isStepInvariant := !ok
		isStepInvariantSlice := make([]bool, len(n.Args))
		for i := range n.Args {
			isStepInvariantSlice[i] = preprocessExprHelper(n.Args[i], start, end)
			isStepInvariant = isStepInvariant && isStepInvariantSlice[i]
		}

		if isStepInvariant {
			// The function and all arguments are step invariant.
			return true
		}

		for i, isi := range isStepInvariantSlice {
			if isi {
				n.Args[i] = newStepInvariantExpr(n.Args[i])
			}
		}
		return false

	case *parser.MatrixSelector:
		return preprocessExprHelper(n.VectorSelector, start, end)

	case *parser.SubqueryExpr:
		// Since we adjust offset for the @ modifier evaluation,
		// it gets tricky to adjust it for every subquery step.
		// Hence we wrap the inside of subquery irrespective of
		// @ on subquery (given it is also step invariant) so that
		// it is evaluated only once w.r.t. the start time of subquery.
		isInvariant := preprocessExprHelper(n.Expr, start, end)
		if isInvariant {
			n.Expr = newStepInvariantExpr(n.Expr)
		}
		if n.StartOrEnd == parser.START {
			n.Timestamp = makeInt64Pointer(timestamp.FromTime(start))
		} else if n.StartOrEnd == parser.END {
			n.Timestamp = makeInt64Pointer(timestamp.FromTime(end))
		}
		return n.Timestamp != nil

	case *parser.ParenExpr:
		return preprocessExprHelper(n.Expr, start, end)

	case *parser.UnaryExpr:
		return preprocessExprHelper(n.Expr, start, end)

	case *parser.NumberLiteral:
		return true
	case *parser.StringLiteral:
		// strings should be used as fixed strings; no need
		// to wrap under stepInvariantExpr
		return false
	}

	panic(fmt.Sprintf("found unexpected node %#v", expr))
}

func makeInt64Pointer(val int64) *int64 {
	valp := new(int64)
	*valp = val
	return valp
}

func newStepInvariantExpr(expr parser.Expr) parser.Expr {
	return &parser.StepInvariantExpr{Expr: expr}
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
