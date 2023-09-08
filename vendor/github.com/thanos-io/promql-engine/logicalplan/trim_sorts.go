// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"github.com/thanos-io/promql-engine/parser"
	"github.com/thanos-io/promql-engine/query"
)

// TrimSortFunctions trims sort functions. It can do that because for nested sort functions
// we can safely say f(sort(X)) == f(X). Top-level sort functions are handled by the engine
// when presenting the query results. The engine depends on this optimizer to be able to ignore
// the 'sort' and 'sort_desc' functions when building its Operator tree.
type TrimSortFunctions struct {
}

func (TrimSortFunctions) Optimize(expr parser.Expr, _ *query.Options) parser.Expr {
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
