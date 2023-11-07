// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"fmt"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
)

type FilteredSelector struct {
	*parser.VectorSelector
	Filters []*labels.Matcher
}

func (f FilteredSelector) String() string {
	return fmt.Sprintf("filter(%s, %s)", f.Filters, f.VectorSelector.String())
}

func (f FilteredSelector) Pretty(level int) string { return f.String() }

func (f FilteredSelector) PositionRange() posrange.PositionRange { return posrange.PositionRange{} }

func (f FilteredSelector) Type() parser.ValueType { return parser.ValueTypeVector }

func (f FilteredSelector) PromQLExpr() {}
