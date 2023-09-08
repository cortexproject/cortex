// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"sort"

	"github.com/thanos-io/promql-engine/parser"
	"github.com/thanos-io/promql-engine/query"
)

// SortMatchers sorts all matchers in a selector so that
// all subsequent optimizers, both in the logical and physical plan,
// can rely on this property.
type SortMatchers struct{}

func (m SortMatchers) Optimize(expr parser.Expr, _ *query.Options) parser.Expr {
	traverse(&expr, func(node *parser.Expr) {
		e, ok := (*node).(*parser.VectorSelector)
		if !ok {
			return
		}

		sort.Slice(e.LabelMatchers, func(i, j int) bool {
			return e.LabelMatchers[i].Name < e.LabelMatchers[j].Name
		})
	})
	return expr
}
