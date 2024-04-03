// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

type NodeType string

const (
	VectorSelectorNode = "vector_selector"
	MatrixSelectorNode = "matrix_selector"
	AggregationNode    = "aggregation"
	BinaryNode         = "binary"
	FunctionNode       = "function"
	NumberLiteralNode  = "number_literal"
	StringLiteralNode  = "string_literal"
	SubqueryNode       = "subquery"
	CheckDuplicateNode = "check_duplicate"
	StepInvariantNode  = "step_invariant"
	ParensNode         = "parens"
	UnaryNode          = "unary"

	RemoteExecutionNode = "remote_exec"
	DeduplicateNode     = "dedup"
	NoopNode            = "noop"
)

type Cloneable interface {
	Clone() Node
}

type Traversable interface {
	Children() []*Node
}

type LeafNode struct{}

func (l LeafNode) Children() []*Node { return nil }

type Node interface {
	fmt.Stringer
	Cloneable
	Traversable
	Type() NodeType
	ReturnType() parser.ValueType
}

type Nodes []Node

// Projection has information on which series labels should be selected from storage.
type Projection struct {
	// Labels is a list of labels to be included or excluded from the selection result, depending on the value of Include.
	Labels []string
	// Include is true if only the provided list of labels should be retrieved from storage.
	// When set to false, the provided list of labels should be excluded from selection.
	Include bool
}

// VectorSelector is vector selector with additional configuration set by optimizers.
type VectorSelector struct {
	*parser.VectorSelector
	LeafNode
	Filters         []*labels.Matcher
	BatchSize       int64
	SelectTimestamp bool
	Projection      Projection
}

func (f *VectorSelector) Clone() Node {
	clone := *f
	vsClone := *f.VectorSelector
	clone.VectorSelector = &vsClone

	clone.Filters = shallowCloneSlice(f.Filters)
	clone.LabelMatchers = shallowCloneSlice(f.LabelMatchers)
	clone.Projection.Labels = shallowCloneSlice(f.Projection.Labels)

	if f.VectorSelector.Timestamp != nil {
		ts := *f.VectorSelector.Timestamp
		clone.Timestamp = &ts
	}

	return &clone
}

func (f *VectorSelector) Type() NodeType { return VectorSelectorNode }

func (f *VectorSelector) String() string {
	if f.SelectTimestamp {
		// If we pushed down timestamp into the vector selector we need to render the proper
		// PromQL again.
		return fmt.Sprintf("timestamp(%s)", f.VectorSelector.String())
	}
	return f.VectorSelector.String()
}

func (f *VectorSelector) ReturnType() parser.ValueType { return parser.ValueTypeVector }

// MatrixSelector is matrix selector with additional configuration set by optimizers.
// It is used so we can get rid of VectorSelector in distributed mode too.
type MatrixSelector struct {
	VectorSelector *VectorSelector `json:"-"`
	Range          time.Duration

	// Needed because this operator is used in the distributed mode
	OriginalString string
}

func (f *MatrixSelector) Clone() Node {
	clone := *f
	clone.VectorSelector = f.VectorSelector.Clone().(*VectorSelector)
	return &clone
}

func (f *MatrixSelector) Children() []*Node {
	var vs Node = f.VectorSelector
	return []*Node{&vs}
}

func (f *MatrixSelector) String() string {
	return f.OriginalString
}

func (f *MatrixSelector) ReturnType() parser.ValueType { return parser.ValueTypeVector }

func (f *MatrixSelector) Type() NodeType { return MatrixSelectorNode }

// CheckDuplicateLabels is a logical node that checks for duplicate labels in the same timestamp.
type CheckDuplicateLabels struct {
	Expr Node `json:"-"`
}

func (c *CheckDuplicateLabels) Clone() Node {
	clone := *c
	clone.Expr = c.Expr.Clone()
	return &clone
}

func (c *CheckDuplicateLabels) Children() []*Node            { return []*Node{&c.Expr} }
func (c *CheckDuplicateLabels) String() string               { return c.Expr.String() }
func (c *CheckDuplicateLabels) ReturnType() parser.ValueType { return c.Expr.ReturnType() }
func (c *CheckDuplicateLabels) Type() NodeType               { return CheckDuplicateNode }

// StringLiteral is a logical node representing a literal string.
type StringLiteral struct {
	LeafNode
	Val string
}

func (c *StringLiteral) Clone() Node                  { return &StringLiteral{Val: c.Val} }
func (c *StringLiteral) String() string               { return fmt.Sprintf("%q", c.Val) }
func (c *StringLiteral) ReturnType() parser.ValueType { return parser.ValueTypeString }
func (c *StringLiteral) Type() NodeType               { return StringLiteralNode }

// NumberLiteral is a logical node representing a literal number.
type NumberLiteral struct {
	LeafNode
	Val float64
}

func (c *NumberLiteral) Clone() Node                  { return &NumberLiteral{Val: c.Val} }
func (c *NumberLiteral) String() string               { return fmt.Sprint(c.Val) }
func (c *NumberLiteral) ReturnType() parser.ValueType { return parser.ValueTypeScalar }
func (c *NumberLiteral) Type() NodeType               { return NumberLiteralNode }

// StepInvariantExpr is a logical node that expresses that the child expression
// returns the same value at every step in the evaluation.
type StepInvariantExpr struct {
	Expr Node `json:"-"`
}

func (c *StepInvariantExpr) Clone() Node {
	clone := *c
	clone.Expr = c.Expr.Clone()
	return &clone
}

func (c *StepInvariantExpr) Children() []*Node            { return []*Node{&c.Expr} }
func (c *StepInvariantExpr) String() string               { return c.Expr.String() }
func (c *StepInvariantExpr) ReturnType() parser.ValueType { return c.Expr.ReturnType() }
func (c *StepInvariantExpr) Type() NodeType               { return StepInvariantNode }

// FunctionCall represents a PromQL function.
type FunctionCall struct {
	// The function that was called.
	Func parser.Function
	// Arguments passed into the function.
	Args []Node `json:"-"`
}

func (f *FunctionCall) Clone() Node {
	clone := *f
	clone.Args = make([]Node, 0, len(f.Args))
	for _, arg := range f.Args {
		clone.Args = append(clone.Args, arg.Clone())
	}
	return &clone
}

func (f *FunctionCall) Children() []*Node {
	args := make([]*Node, 0, len(f.Args))
	for i := range f.Args {
		args = append(args, &f.Args[i])
	}
	return args
}

func (f *FunctionCall) String() string {
	args := make([]string, 0, len(f.Args))
	for _, arg := range f.Args {
		args = append(args, arg.String())
	}
	return fmt.Sprintf("%s(%s)", f.Func.Name, strings.Join(args, ", "))
}

func (f *FunctionCall) ReturnType() parser.ValueType { return f.Func.ReturnType }
func (f *FunctionCall) Type() NodeType               { return FunctionNode }

type Parens struct {
	Expr Node `json:"-"`
}

func (p *Parens) Clone() Node                  { return &Parens{Expr: p.Expr.Clone()} }
func (p *Parens) Children() []*Node            { return []*Node{&p.Expr} }
func (p *Parens) String() string               { return fmt.Sprintf("(%s)", p.Expr.String()) }
func (p *Parens) ReturnType() parser.ValueType { return p.Expr.ReturnType() }
func (p *Parens) Type() NodeType               { return ParensNode }

type Unary struct {
	Op   parser.ItemType
	Expr Node `json:"-"`
}

func (p *Unary) Clone() Node                  { return &Unary{Op: p.Op, Expr: p.Expr.Clone()} }
func (p *Unary) Children() []*Node            { return []*Node{&p.Expr} }
func (p *Unary) String() string               { return fmt.Sprintf("%s%s", p.Op.String(), p.Expr.String()) }
func (p *Unary) ReturnType() parser.ValueType { return p.Expr.ReturnType() }
func (p *Unary) Type() NodeType               { return UnaryNode }

// Aggregation represents a PromQL aggregation.
type Aggregation struct {
	Op       parser.ItemType
	Expr     Node `json:"-"`
	Param    Node `json:"-"`
	Grouping []string
	Without  bool
}

func (f *Aggregation) Clone() Node {
	clone := *f
	clone.Expr = f.Expr.Clone()
	if clone.Param != nil {
		clone.Param = f.Param.Clone()
	}
	clone.Grouping = shallowCloneSlice(f.Grouping)
	return &clone
}

func (f *Aggregation) Children() []*Node {
	children := []*Node{&f.Expr}
	if f.Param != nil {
		children = append(children, &f.Param)
	}
	return children
}

func (f *Aggregation) String() string {
	aggrString := f.getAggOpStr()
	aggrString += "("
	if f.Op.IsAggregatorWithParam() {
		aggrString += fmt.Sprintf("%s, ", f.Param)
	}
	aggrString += fmt.Sprintf("%s)", f.Expr)

	return aggrString
}

func (f *Aggregation) ReturnType() parser.ValueType { return parser.ValueTypeVector }
func (f *Aggregation) Type() NodeType               { return AggregationNode }

func (f *Aggregation) getAggOpStr() string {
	aggrString := f.Op.String()

	switch {
	case f.Without:
		aggrString += fmt.Sprintf(" without (%s) ", strings.Join(f.Grouping, ", "))
	case len(f.Grouping) > 0:
		aggrString += fmt.Sprintf(" by (%s) ", strings.Join(f.Grouping, ", "))
	}

	return aggrString
}

type Binary struct {
	Op       parser.ItemType // The operation of the expression.
	LHS, RHS Node            `json:"-"` // The operands on the respective sides of the operator.

	// The matching behavior for the operation if both operands are Vectors.
	// If they are not this field is nil.
	VectorMatching *parser.VectorMatching

	// If a comparison operator, return 0/1 rather than filtering.
	ReturnBool bool

	ValueType parser.ValueType
}

func (b *Binary) Clone() Node {
	clone := *b
	clone.LHS = b.LHS.Clone()
	clone.RHS = b.RHS.Clone()
	if b.VectorMatching != nil {
		vm := *b.VectorMatching
		clone.VectorMatching = &vm
	}
	return &clone
}

func (b *Binary) Children() []*Node { return []*Node{&b.LHS, &b.RHS} }

func (b *Binary) ReturnType() parser.ValueType {
	if b.LHS.ReturnType() == parser.ValueTypeScalar && b.RHS.ReturnType() == parser.ValueTypeScalar {
		return parser.ValueTypeScalar
	}
	return parser.ValueTypeVector
}

func (b *Binary) Type() NodeType { return BinaryNode }

func (b *Binary) String() string {
	returnBool := ""
	if b.ReturnBool {
		returnBool = " bool"
	}

	matching := b.getMatchingStr()
	return fmt.Sprintf("%s %s%s%s %s", b.LHS, b.Op, returnBool, matching, b.RHS)
}

func (b *Binary) getMatchingStr() string {
	matching := ""
	vm := b.VectorMatching
	if vm != nil && (len(vm.MatchingLabels) > 0 || vm.On) {
		vmTag := "ignoring"
		if vm.On {
			vmTag = "on"
		}
		matching = fmt.Sprintf(" %s (%s)", vmTag, strings.Join(vm.MatchingLabels, ", "))

		if vm.Card == parser.CardManyToOne || vm.Card == parser.CardOneToMany {
			vmCard := "right"
			if vm.Card == parser.CardManyToOne {
				vmCard = "left"
			}
			matching += fmt.Sprintf(" group_%s (%s)", vmCard, strings.Join(vm.Include, ", "))
		}
	}
	return matching
}

type Subquery struct {
	Expr  Node `json:"-"`
	Range time.Duration
	// OriginalOffset is the actual offset that was set in the query.
	// This never changes.
	OriginalOffset time.Duration
	// Offset is the offset used during the query execution
	// which is calculated using the original offset, at modifier time,
	// eval time, and subquery offsets in the AST tree.
	Offset    time.Duration
	Timestamp *int64
	Step      time.Duration

	StartOrEnd parser.ItemType
}

func (s *Subquery) Clone() Node {
	clone := *s
	clone.Expr = s.Expr.Clone()

	if s.Timestamp != nil {
		ts := *s.Timestamp
		clone.Timestamp = &ts
	}

	return &clone
}

func (s *Subquery) Children() []*Node { return []*Node{&s.Expr} }

func (s *Subquery) String() string {
	return fmt.Sprintf("%s%s", s.Expr.String(), s.getSubqueryTimeSuffix())
}

func (s *Subquery) ReturnType() parser.ValueType { return s.Expr.ReturnType() }

func (s *Subquery) Type() NodeType { return SubqueryNode }

func (s *Subquery) getSubqueryTimeSuffix() any {
	step := ""
	if s.Step != 0 {
		step = model.Duration(s.Step).String()
	}
	offset := ""
	switch {
	case s.OriginalOffset > time.Duration(0):
		offset = fmt.Sprintf(" offset %s", model.Duration(s.OriginalOffset))
	case s.OriginalOffset < time.Duration(0):
		offset = fmt.Sprintf(" offset -%s", model.Duration(-s.OriginalOffset))
	}
	at := ""
	switch {
	case s.Timestamp != nil:
		at = fmt.Sprintf(" @ %.3f", float64(*s.Timestamp)/1000.0)
	case s.StartOrEnd == parser.START:
		at = " @ start()"
	case s.StartOrEnd == parser.END:
		at = " @ end()"
	}
	return fmt.Sprintf("[%s:%s]%s%s", model.Duration(s.Range), step, at, offset)
}

func shallowCloneSlice[T any](s []T) []T {
	if s == nil {
		return nil
	}
	clone := make([]T, len(s))
	copy(clone, s)
	return clone
}
