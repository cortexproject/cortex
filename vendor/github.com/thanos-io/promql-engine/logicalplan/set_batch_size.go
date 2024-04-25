// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/thanos-io/promql-engine/query"
)

// SelectorBatchSize configures the batch size of selector based on
// aggregates present in the plan.
type SelectorBatchSize struct {
	Size int64
}

// Optimize configures the batch size of selector based on the query plan.
// If any aggregate is present in the plan, the batch size is set to the configured value.
// The two exceptions where this cannot be done is if the aggregate is quantile, or
// when a binary expression precedes the aggregate.
func (m SelectorBatchSize) Optimize(plan Node, _ *query.Options) (Node, annotations.Annotations) {
	canBatch := false
	Traverse(&plan, func(current *Node) {
		switch e := (*current).(type) {
		case *FunctionCall:
			//TODO: calls can reduce the labelset of the input; think histogram_quantile reducing
			// multiple "le" labels into one output. We cannot handle this in batching. Revisit
			// what is safe here.
			canBatch = false
		case *Binary:
			canBatch = false
		case *Aggregation:
			if e.Op == parser.QUANTILE || e.Op == parser.TOPK || e.Op == parser.BOTTOMK {
				canBatch = false
				return
			}
			canBatch = true
		case *VectorSelector:
			if canBatch {
				e.BatchSize = m.Size
			}
			canBatch = false
		}
	})
	return plan, nil
}
