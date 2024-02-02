// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
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
	Expr() parser.Expr
}

type Optimizer interface {
	Optimize(plan parser.Expr, opts *query.Options) (parser.Expr, annotations.Annotations)
}

type plan struct {
	expr     parser.Expr
	opts     *query.Options
	planOpts PlanOptions
}

type PlanOptions struct {
	DisableDuplicateLabelCheck bool
}

func New(expr parser.Expr, queryOpts *query.Options, planOpts PlanOptions) Plan {
	expr = preprocessExpr(expr, queryOpts.Start, queryOpts.End)
	setOffsetForAtModifier(queryOpts.Start.UnixMilli(), expr)
	setOffsetForInnerSubqueries(expr, queryOpts)

	// the engine handles sorting at the presentation layer
	expr = trimSorts(expr)

	// replace scanners by our logical nodes
	expr = replaceSelectors(expr)

	return &plan{
		expr:     expr,
		opts:     queryOpts,
		planOpts: planOpts,
	}
}

func (p *plan) Optimize(optimizers []Optimizer) (Plan, annotations.Annotations) {
	annos := annotations.New()
	for _, o := range optimizers {
		var a annotations.Annotations
		p.expr, a = o.Optimize(p.expr, p.opts)
		annos.Merge(a)
	}
	// parens are just annoying and getting rid of them doesn't change the query
	expr := trimParens(p.expr)

	if !p.planOpts.DisableDuplicateLabelCheck {
		expr = insertDuplicateLabelChecks(expr)
	}

	return &plan{expr: expr, opts: p.opts}, *annos
}

func (p *plan) Expr() parser.Expr {
	return p.expr
}

func traverse(expr *parser.Expr, transform func(*parser.Expr)) {
	switch node := (*expr).(type) {
	case *parser.StepInvariantExpr:
		transform(expr)
		traverse(&node.Expr, transform)
	case *parser.VectorSelector:
		transform(expr)
	case *VectorSelector:
		var x parser.Expr = node.VectorSelector
		transform(expr)
		traverse(&x, transform)
	case *MatrixSelector:
		var x parser.Expr = node.MatrixSelector
		transform(expr)
		traverse(&x, transform)
	case *parser.MatrixSelector:
		transform(expr)
		traverse(&node.VectorSelector, transform)
	case *parser.AggregateExpr:
		transform(expr)
		traverse(&node.Param, transform)
		traverse(&node.Expr, transform)
	case *parser.Call:
		transform(expr)
		for i := range node.Args {
			traverse(&(node.Args[i]), transform)
		}
	case *parser.BinaryExpr:
		transform(expr)
		traverse(&node.LHS, transform)
		traverse(&node.RHS, transform)
	case *parser.UnaryExpr:
		transform(expr)
		traverse(&node.Expr, transform)
	case *parser.ParenExpr:
		transform(expr)
		traverse(&node.Expr, transform)
	case *parser.SubqueryExpr:
		transform(expr)
		traverse(&node.Expr, transform)
	case CheckDuplicateLabels:
		transform(expr)
		traverse(&node.Expr, transform)
	}
}

func TraverseBottomUp(parent *parser.Expr, current *parser.Expr, transform func(parent *parser.Expr, node *parser.Expr) bool) bool {
	switch node := (*current).(type) {
	case *parser.StringLiteral:
		return false
	case *parser.NumberLiteral:
		return false
	case *parser.StepInvariantExpr:
		if stop := TraverseBottomUp(current, &node.Expr, transform); stop {
			return stop
		}
		return transform(parent, current)
	case *parser.VectorSelector:
		return transform(parent, current)
	case *VectorSelector:
		if stop := transform(parent, current); stop {
			return stop
		}
		var x parser.Expr = node.VectorSelector
		return TraverseBottomUp(current, &x, transform)
	case *MatrixSelector:
		if stop := transform(parent, current); stop {
			return stop
		}
		var x parser.Expr = node.MatrixSelector
		return TraverseBottomUp(current, &x, transform)
	case *parser.MatrixSelector:
		return transform(current, &node.VectorSelector)
	case *parser.AggregateExpr:
		if stop := TraverseBottomUp(current, &node.Expr, transform); stop {
			return stop
		}
		return transform(parent, current)
	case *parser.Call:
		for i := range node.Args {
			if stop := TraverseBottomUp(current, &node.Args[i], transform); stop {
				return stop
			}
		}
		return transform(parent, current)
	case *parser.BinaryExpr:
		lstop := TraverseBottomUp(current, &node.LHS, transform)
		rstop := TraverseBottomUp(current, &node.RHS, transform)
		if lstop || rstop {
			return true
		}
		return transform(parent, current)
	case *parser.UnaryExpr:
		return TraverseBottomUp(current, &node.Expr, transform)
	case *parser.ParenExpr:
		if stop := TraverseBottomUp(current, &node.Expr, transform); stop {
			return stop
		}
		return transform(parent, current)
	case *parser.SubqueryExpr:
		if stop := TraverseBottomUp(current, &node.Expr, transform); stop {
			return stop
		}
		return transform(parent, current)
	case CheckDuplicateLabels:
		if stop := TraverseBottomUp(current, &node.Expr, transform); stop {
			return stop
		}
		return transform(parent, current)
	}
	return true
}

func replaceSelectors(plan parser.Expr) parser.Expr {
	traverse(&plan, func(current *parser.Expr) {
		switch t := (*current).(type) {
		case *parser.MatrixSelector:
			*current = &MatrixSelector{MatrixSelector: t, OriginalString: t.String()}
		case *parser.VectorSelector:
			*current = &VectorSelector{VectorSelector: t}
		case *parser.Call:
			if t.Func.Name != "timestamp" {
				return
			}
			switch v := unwrapParens(t.Args[0]).(type) {
			case *parser.VectorSelector:
				*current = &VectorSelector{VectorSelector: v, SelectTimestamp: true}
			case *parser.StepInvariantExpr:
				vs, ok := unwrapParens(v.Expr).(*parser.VectorSelector)
				if ok {
					// Prometheus weirdness
					if vs.Timestamp != nil {
						vs.OriginalOffset = 0
					}
					*current = &VectorSelector{VectorSelector: vs, SelectTimestamp: true}
				}
			}
		}
	})
	return plan
}

func trimSorts(expr parser.Expr) parser.Expr {
	canTrimSorts := true
	// We cannot trim inner sort if its an argument to a timestamp function.
	// If we would do it we could transform "timestamp(sort(X))" into "timestamp(X)"
	// Which might return actual timestamps of samples instead of query execution timestamp.
	TraverseBottomUp(nil, &expr, func(parent, current *parser.Expr) bool {
		if current == nil || parent == nil {
			return true
		}
		e, pok := (*parent).(*parser.Call)
		f, cok := (*current).(*parser.Call)

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
	TraverseBottomUp(nil, &expr, func(parent, current *parser.Expr) bool {
		if current == nil || parent == nil {
			return true
		}
		switch e := (*parent).(type) {
		case *parser.Call:
			switch e.Func.Name {
			case "sort", "sort_desc":
				*parent = *current
			}
		}
		return false
	})
	return expr
}

func trimParens(expr parser.Expr) parser.Expr {
	TraverseBottomUp(nil, &expr, func(parent, current *parser.Expr) bool {
		if current == nil || parent == nil {
			return true
		}
		switch (*parent).(type) {
		case *parser.ParenExpr:
			*parent = *current
		}
		return false
	})
	return expr
}

func insertDuplicateLabelChecks(expr parser.Expr) parser.Expr {
	traverse(&expr, func(node *parser.Expr) {
		switch t := (*node).(type) {
		case *parser.AggregateExpr, *parser.UnaryExpr, *parser.BinaryExpr, *parser.Call:
			*node = CheckDuplicateLabels{Expr: t}
		case *VectorSelector:
			if t.SelectTimestamp {
				*node = CheckDuplicateLabels{Expr: t}
			}
		}
	})
	return expr
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

func unwrapParens(expr parser.Expr) parser.Expr {
	switch t := expr.(type) {
	case *parser.ParenExpr:
		return unwrapParens(t.Expr)
	default:
		return t
	}
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
		nOpts := query.NestedOptionsForSubquery(opts, n)
		setOffsetForAtModifier(nOpts.Start.UnixMilli(), n.Expr)
		setOffsetForInnerSubqueries(n.Expr, nOpts)
	default:
		for _, c := range parser.Children(n) {
			setOffsetForInnerSubqueries(c.(parser.Expr), opts)
		}
	}
}

// VectorSelector is vector selector with additional configuration set by optimizers.
type VectorSelector struct {
	*parser.VectorSelector
	Filters         []*labels.Matcher
	BatchSize       int64
	SelectTimestamp bool
}

func (f VectorSelector) String() string {
	if f.SelectTimestamp {
		// If we pushed down timestamp into the vector selector we need to render the proper
		// PromQL again.
		return fmt.Sprintf("timestamp(%s)", f.VectorSelector.String())
	}
	return f.VectorSelector.String()
}

func (f VectorSelector) Pretty(level int) string { return f.String() }

func (f VectorSelector) PositionRange() posrange.PositionRange { return posrange.PositionRange{} }

func (f VectorSelector) Type() parser.ValueType { return parser.ValueTypeVector }

func (f VectorSelector) PromQLExpr() {}

// MatrixSelector is matrix selector with additional configuration set by optimizers.
// It is used so we can get rid of VectorSelector in distributed mode too.
type MatrixSelector struct {
	*parser.MatrixSelector

	// Needed because this operator is used in the distributed mode
	OriginalString string
}

func (f MatrixSelector) String() string {
	return f.OriginalString
}

func (f MatrixSelector) Pretty(level int) string { return f.String() }

func (f MatrixSelector) PositionRange() posrange.PositionRange { return posrange.PositionRange{} }

func (f MatrixSelector) Type() parser.ValueType { return parser.ValueTypeVector }

func (f MatrixSelector) PromQLExpr() {}

type CheckDuplicateLabels struct {
	Expr parser.Expr
}

func (c CheckDuplicateLabels) String() string {
	return c.Expr.String()
}

func (c CheckDuplicateLabels) Pretty(level int) string { return c.Expr.Pretty(level) }

func (c CheckDuplicateLabels) PositionRange() posrange.PositionRange { return c.Expr.PositionRange() }

func (c CheckDuplicateLabels) Type() parser.ValueType { return c.Expr.Type() }

func (c CheckDuplicateLabels) PromQLExpr() {}
