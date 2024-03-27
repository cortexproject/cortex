// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
)

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
	Filters         []*labels.Matcher
	BatchSize       int64
	SelectTimestamp bool
	Projection      Projection
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
	VectorSelector parser.Expr
	Range          time.Duration

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

// CheckDuplicateLabels is a logical node that checks for duplicate labels in the same timestamp.
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

// StringLiteral is a logical node representing a literal string.
type StringLiteral struct {
	Val string
}

func (c StringLiteral) String() string {
	return fmt.Sprintf("%q", c.Val)
}

func (c StringLiteral) Pretty(level int) string { return c.String() }

func (c StringLiteral) PositionRange() posrange.PositionRange { return posrange.PositionRange{} }

func (c StringLiteral) Type() parser.ValueType { return parser.ValueTypeString }

func (c StringLiteral) PromQLExpr() {}

// NumberLiteral is a logical node representing a literal number.
type NumberLiteral struct {
	Val float64
}

func (c NumberLiteral) String() string {
	return fmt.Sprint(c.Val)
}

func (c NumberLiteral) Pretty(level int) string { return c.String() }

func (c NumberLiteral) PositionRange() posrange.PositionRange { return posrange.PositionRange{} }

func (c NumberLiteral) Type() parser.ValueType { return parser.ValueTypeScalar }

func (c NumberLiteral) PromQLExpr() {}

// StepInvariantExpr is a logical node that expresses that the child expression
// returns the same value at every step in the evaluation.
type StepInvariantExpr struct {
	Expr parser.Expr
}

func (c StepInvariantExpr) String() string { return c.Expr.String() }

func (c StepInvariantExpr) Pretty(level int) string { return c.String() }

func (c StepInvariantExpr) PositionRange() posrange.PositionRange {
	return c.Expr.PositionRange()
}

func (c StepInvariantExpr) Type() parser.ValueType { return c.Expr.Type() }

func (c StepInvariantExpr) PromQLExpr() {}
