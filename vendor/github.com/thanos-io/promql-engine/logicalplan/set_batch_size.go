// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"github.com/thanos-io/promql-engine/query"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/annotations"
)

// SelectorBatchSize configures the batch size of selector based on
// aggregates present in the plan.
type SelectorBatchSize struct {
	Size int64
}

// Optimize configures the batch size of selector based on the query plan.
// It recursively traverses the plan and sets BatchSize on VectorSelectors
// that can safely produce results in batches. Batching is enabled when an
// Aggregation is encountered and propagated down through nodes that are
// per-series independent (range functions, set operations, scalar-vector
// arithmetic). Batching is disabled by nodes that require cross-series
// visibility (group_left/group_right, histogram_quantile, topk, etc.).
func (m SelectorBatchSize) Optimize(plan Node, _ *query.Options) (Node, annotations.Annotations) {
	m.setBatchSize(&plan, false)
	return plan, nil
}

func (m SelectorBatchSize) setBatchSize(node *Node, canBatch bool) {
	switch e := (*node).(type) {
	case *Aggregation:
		if e.Op == parser.QUANTILE || e.Op == parser.TOPK || e.Op == parser.BOTTOMK || e.Op == parser.LIMITK || e.Op == parser.LIMIT_RATIO {
			canBatch = false
		} else {
			canBatch = true
		}
	case *FunctionCall:
		// Range vector functions (rate, increase, present_over_time, etc.) are safe for
		// batching because each output series depends only on that series' own range data.
		// The function receives a single series' samples over the [range] window and produces
		// one output value — no cross-series state is needed.
		//
		// Instant vector functions (e.g., histogram_quantile) may reduce or combine series
		// (e.g., merging multiple "le" labels into one output), requiring full series visibility.
		// We disable batching for those.
		if !isRangeVectorFunction(e) {
			canBatch = false
		}
	case *Binary:
		if isManyToOneOrOneToMany(e) {
			canBatch = false
		}
	case *VectorSelector:
		if canBatch {
			e.BatchSize = m.Size
		}
		return
	}

	for _, child := range (*node).Children() {
		m.setBatchSize(child, canBatch)
	}
}

func isRangeVectorFunction(f *FunctionCall) bool {
	for _, arg := range f.Args {
		switch arg.(type) {
		case *MatrixSelector, *Subquery:
			return true
		}
	}
	return false
}

// isManyToOneOrOneToMany returns true for binary operations with group_left/group_right
// matching, which require full series visibility for correct join behavior.
// All other binary operations (set ops, one-to-one, scalar-vector) are safe with batching.
func isManyToOneOrOneToMany(b *Binary) bool {
	return b.VectorMatching != nil &&
		(b.VectorMatching.Card == parser.CardManyToOne || b.VectorMatching.Card == parser.CardOneToMany)
}
