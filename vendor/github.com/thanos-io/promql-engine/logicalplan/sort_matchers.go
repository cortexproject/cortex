// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"sort"

	"github.com/prometheus/prometheus/util/annotations"

	"github.com/thanos-io/promql-engine/query"
)

// SortMatchers sorts all matchers in a selector so that
// all subsequent optimizers, both in the logical and physical plan,
// can rely on this property.
type SortMatchers struct{}

func (m SortMatchers) Optimize(plan Node, _ *query.Options) (Node, annotations.Annotations) {
	Traverse(&plan, func(node *Node) {
		e, ok := (*node).(*VectorSelector)
		if !ok {
			return
		}

		sort.Slice(e.LabelMatchers, func(i, j int) bool {
			return e.LabelMatchers[i].Name < e.LabelMatchers[j].Name
		})
	})
	return plan, nil
}
