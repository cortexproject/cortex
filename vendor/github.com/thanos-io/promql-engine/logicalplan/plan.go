// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"math"
	"strings"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/thanos-io/promql-engine/query"
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
	Optimize([]Optimizer) (Plan, annotations.Annotations)
	Root() Node
}

type Optimizer interface {
	Optimize(plan Node, opts *query.Options) (Node, annotations.Annotations)
}

type plan struct {
	expr     Node
	opts     *query.Options
	planOpts PlanOptions
}

type PlanOptions struct {
	DisableDuplicateLabelCheck bool
}

// New creates a new logical plan from logical node.
func New(root Node, queryOpts *query.Options, planOpts PlanOptions) Plan {
	return &plan{
		expr:     root,
		opts:     queryOpts,
		planOpts: planOpts,
	}
}

func NewFromAST(ast parser.Expr, queryOpts *query.Options, planOpts PlanOptions) Plan {
	ast = promql.PreprocessExpr(ast, queryOpts.Start, queryOpts.End)
	setOffsetForAtModifier(queryOpts.Start.UnixMilli(), ast)
	setOffsetForInnerSubqueries(ast, queryOpts)

	// replace scanners by our logical nodes
	expr := replacePrometheusNodes(ast)

	// the engine handles sorting at the presentation layer
	expr = trimSorts(expr)

	return &plan{
		expr:     expr,
		opts:     queryOpts,
		planOpts: planOpts,
	}
}

// NewFromBytes creates a new logical plan from a byte slice created with Marshal.
// This method is used to deserialize a logical plan which has been sent over the wire.
func NewFromBytes(bytes []byte, queryOpts *query.Options, planOpts PlanOptions) (Plan, error) {
	root, err := Unmarshal(bytes)
	if err != nil {
		return nil, err
	}
	// the engine handles sorting at the presentation layer
	root = trimSorts(root)

	return &plan{
		expr:     root,
		opts:     queryOpts,
		planOpts: planOpts,
	}, nil
}

func (p *plan) Optimize(optimizers []Optimizer) (Plan, annotations.Annotations) {
	annos := annotations.New()
	for _, o := range optimizers {
		var a annotations.Annotations
		p.expr, a = o.Optimize(p.expr, p.opts)
		annos.Merge(a)
	}
	// parens are just annoying and getting rid of them doesn't change the query
	// NOTE: we need to do this here to not break the distributed optimizer since
	// rendering subqueries String() method depends on parens sometimes.
	expr := trimParens(p.expr)

	if !p.planOpts.DisableDuplicateLabelCheck {
		expr = insertDuplicateLabelChecks(expr)
	}

	return &plan{expr: expr, opts: p.opts}, *annos
}

func (p *plan) Root() Node {
	return p.expr
}

func Traverse(expr *Node, transform func(*Node)) {
	children := (*expr).Children()
	transform(expr)
	for _, c := range children {
		Traverse(c, transform)
	}
}

func TraverseBottomUp(parent *Node, current *Node, transform func(parent *Node, node *Node) bool) bool {
	var stop bool
	for _, c := range (*current).Children() {
		stop = TraverseBottomUp(current, c, transform) || stop
	}
	if stop {
		return stop
	}
	return transform(parent, current)
}

func replacePrometheusNodes(plan parser.Expr) Node {
	switch t := (plan).(type) {
	case *parser.StringLiteral:
		return &StringLiteral{Val: t.Val}
	case *parser.NumberLiteral:
		return &NumberLiteral{Val: t.Val}
	case *parser.StepInvariantExpr:
		return &StepInvariantExpr{Expr: replacePrometheusNodes(t.Expr)}
	case *parser.MatrixSelector:
		return &MatrixSelector{
			VectorSelector: &VectorSelector{
				VectorSelector: t.VectorSelector.(*parser.VectorSelector),
			},
			Range:          t.Range,
			OriginalString: t.String(),
		}
	case *parser.VectorSelector:
		return &VectorSelector{VectorSelector: t}

	// TODO: we dont yet have logical nodes for these, keep traversing here but set fields in-place
	case *parser.Call:
		if t.Func.Name == "timestamp" {
			// pushed-down timestamp function
			switch v := UnwrapParens(t.Args[0]).(type) {
			case *parser.VectorSelector:
				return &VectorSelector{VectorSelector: v, SelectTimestamp: true}
			case *parser.StepInvariantExpr:
				vs, ok := UnwrapParens(v.Expr).(*parser.VectorSelector)
				if ok {
					// Prometheus weirdness
					if vs.Timestamp != nil {
						vs.OriginalOffset = 0
					}
					return &StepInvariantExpr{
						Expr: &VectorSelector{VectorSelector: vs, SelectTimestamp: true},
					}
				}
			}
		}
		args := make([]Node, len(t.Args))
		// nested timestamp functions
		for i, arg := range t.Args {
			args[i] = replacePrometheusNodes(arg)
		}
		return &FunctionCall{
			Func: *t.Func,
			Args: args,
		}
	case *parser.ParenExpr:
		return &Parens{
			Expr: replacePrometheusNodes(t.Expr),
		}
	case *parser.UnaryExpr:
		return &Unary{
			Op:   t.Op,
			Expr: replacePrometheusNodes(t.Expr),
		}
	case *parser.AggregateExpr:
		return &Aggregation{
			Op:       t.Op,
			Expr:     replacePrometheusNodes(t.Expr),
			Param:    replacePrometheusNodes(t.Param),
			Grouping: t.Grouping,
			Without:  t.Without,
		}
	case *parser.BinaryExpr:
		return &Binary{
			Op:             t.Op,
			LHS:            replacePrometheusNodes(t.LHS),
			RHS:            replacePrometheusNodes(t.RHS),
			VectorMatching: t.VectorMatching,
			ReturnBool:     t.ReturnBool,
		}
	case *parser.SubqueryExpr:
		return &Subquery{
			Expr:           replacePrometheusNodes(t.Expr),
			Range:          t.Range,
			OriginalOffset: t.OriginalOffset,
			Offset:         t.Offset,
			Timestamp:      t.Timestamp,
			Step:           t.Step,
			StartOrEnd:     t.StartOrEnd,
		}
	case nil:
		return nil
	}
	panic("Unrecognized AST node")
}

func trimSorts(expr Node) Node {
	canTrimSorts := true
	// We cannot trim inner sort if its an argument to a timestamp function.
	// If we would do it we could transform "timestamp(sort(X))" into "timestamp(X)"
	// Which might return actual timestamps of samples instead of query execution timestamp.
	TraverseBottomUp(nil, &expr, func(parent, current *Node) bool {
		if current == nil || parent == nil {
			return true
		}
		e, pok := (*parent).(*FunctionCall)
		f, cok := (*current).(*FunctionCall)

		if pok && cok {
			if e.Func.Name == "timestamp" && strings.HasPrefix(f.Func.Name, "sort") {
				canTrimSorts = false
				return true
			}
		}
		return false
	})
	if !canTrimSorts {
		return expr
	}
	TraverseBottomUp(nil, &expr, func(parent, current *Node) bool {
		if current == nil || parent == nil {
			return true
		}
		switch e := (*parent).(type) {
		case *FunctionCall:
			switch e.Func.Name {
			case "sort", "sort_desc":
				*parent = *current
			}
		}
		return false
	})
	return expr
}

func trimParens(expr Node) Node {
	TraverseBottomUp(nil, &expr, func(parent, current *Node) bool {
		if current == nil || parent == nil {
			return true
		}
		switch (*parent).(type) {
		case *Parens:
			*parent = *current
		}
		return false
	})
	return expr
}

func insertDuplicateLabelChecks(expr Node) Node {
	Traverse(&expr, func(node *Node) {
		switch t := (*node).(type) {
		case *CheckDuplicateLabels:
			return
		case *Aggregation, *Unary, *Binary, *FunctionCall:
			*node = &CheckDuplicateLabels{Expr: t}
		case *VectorSelector:
			if t.SelectTimestamp {
				*node = &CheckDuplicateLabels{Expr: t}
			}
		}
	})
	return expr
}

// Copy from https://github.com/prometheus/prometheus/blob/v2.39.1/promql/engine.go#L2658.
func setOffsetForAtModifier(evalTime int64, expr parser.Expr) {
	getOffset := func(ts *int64, originalOffset time.Duration, path []parser.Node) time.Duration {
		if ts == nil {
			return originalOffset
		}
		subqOffset, _, subqTs := subqueryTimes(path)
		if subqTs != nil {
			subqOffset += time.Duration(evalTime-*subqTs) * time.Millisecond
		}

		offsetForTs := time.Duration(evalTime-*ts) * time.Millisecond
		offsetDiff := offsetForTs - subqOffset
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

// https://github.com/prometheus/prometheus/blob/dfae954dc1137568f33564e8cffda321f2867925/promql/engine.go#L754
// subqueryTimes returns the sum of offsets and ranges of all subqueries in the path.
// If the @ modifier is used, then the offset and range is w.r.t. that timestamp
// (i.e. the sum is reset when we have @ modifier).
// The returned *int64 is the closest timestamp that was seen. nil for no @ modifier.
func subqueryTimes(path []parser.Node) (time.Duration, time.Duration, *int64) {
	var (
		subqOffset, subqRange time.Duration
		ts                    int64 = math.MaxInt64
	)
	for _, node := range path {
		if n, ok := node.(*parser.SubqueryExpr); ok {
			subqOffset += n.OriginalOffset
			subqRange += n.Range
			if n.Timestamp != nil {
				// The @ modifier on subquery invalidates all the offset and
				// range till now. Hence resetting it here.
				subqOffset = n.OriginalOffset
				subqRange = n.Range
				ts = *n.Timestamp
			}
		}
	}
	var tsp *int64
	if ts != math.MaxInt64 {
		tsp = &ts
	}
	return subqOffset, subqRange, tsp
}

func setOffsetForInnerSubqueries(expr parser.Expr, opts *query.Options) {
	switch n := expr.(type) {
	case *parser.SubqueryExpr:
		nOpts := query.NestedOptionsForSubquery(opts, n.Step, n.Range, n.Offset)
		setOffsetForAtModifier(nOpts.Start.UnixMilli(), n.Expr)
		setOffsetForInnerSubqueries(n.Expr, nOpts)
	default:
		for _, c := range parser.Children(n) {
			setOffsetForInnerSubqueries(c.(parser.Expr), opts)
		}
	}
}
