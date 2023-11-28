// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"fmt"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
)

// VectorSelector is vector selector with additional configuration set by optimizers.
// TODO(fpetkovski): Consider replacing all VectorSelector nodes with this one as the first step in the plan.
// This should help us avoid dealing with both types in the entire codebase.
type VectorSelector struct {
	*parser.VectorSelector
	Filters   []*labels.Matcher
	BatchSize int64
}

func (f VectorSelector) String() string {
	if f.BatchSize != 0 && len(f.Filters) != 0 {
		return fmt.Sprintf("filter(%s, %s[batch=%d])", f.Filters, f.VectorSelector.String(), f.BatchSize)
	}
	if f.BatchSize != 0 {
		return fmt.Sprintf("%s[batch=%d]", f.VectorSelector.String(), f.BatchSize)
	}
	return fmt.Sprintf("filter(%s, %s)", f.Filters, f.VectorSelector.String())
}

func (f VectorSelector) Pretty(level int) string { return f.String() }

func (f VectorSelector) PositionRange() posrange.PositionRange { return posrange.PositionRange{} }

func (f VectorSelector) Type() parser.ValueType { return parser.ValueTypeVector }

func (f VectorSelector) PromQLExpr() {}
