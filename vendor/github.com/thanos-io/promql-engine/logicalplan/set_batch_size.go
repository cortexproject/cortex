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
func (m SelectorBatchSize) Optimize(plan parser.Expr, _ *query.Options) (parser.Expr, annotations.Annotations) {
	canBatch := false
	traverse(&plan, func(current *parser.Expr) {
		switch e := (*current).(type) {
		case *parser.Call:
			canBatch = e.Func.Name == "histogram_quantile"
		case *parser.BinaryExpr:
			canBatch = false
		case *parser.AggregateExpr:
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
		case *parser.VectorSelector:
			if canBatch {
				*current = &VectorSelector{VectorSelector: e, BatchSize: m.Size}
			}
			canBatch = false
		}
	})
	return plan, nil
}
