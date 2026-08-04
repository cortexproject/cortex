// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"github.com/thanos-io/promql-engine/query"

	"github.com/prometheus/prometheus/util/annotations"
)

type DetectHistogramStatsOptimizer struct{}

func (DetectHistogramStatsOptimizer) Optimize(plan Node, _ *query.Options) (Node, annotations.Annotations) {
	TraverseWithParents(nil, &plan, func(parents []*Node, node *Node) {
		n, ok := (*node).(*VectorSelector)
		if !ok {
			return
		}

		n.DecodeNativeHistogramStats = false

	pathLoop:
		for i := len(parents) - 1; i >= 0; i-- { // Walk backwards up the path.
			switch p := (*parents[i]).(type) {
			case *Subquery:
				// If we ever see a subquery in the path, we will not skip
				// the buckets. We need the buckets for correct counter reset
				// detection.
				n.DecodeNativeHistogramStats = false
				break pathLoop

			case *FunctionCall:
				switch p.Func.Name {
				case "histogram_count", "histogram_sum", "histogram_avg":
					// We allow skipping buckets preliminarily. But we continue
					// walking up the path to see if we find a subquery (or a
					// histogram function) further up.
					n.DecodeNativeHistogramStats = true
				case "histogram_quantile", "histogram_quantiles", "histogram_fraction":
					// If we ever see a function that needs the whole histogram,
					// we will not skip the buckets.
					n.DecodeNativeHistogramStats = false
					break pathLoop
				}

			case *Binary:
				if op := p.Op.String(); op == "</" || op == ">/" {
					// Trimming depends on buckets, we will not skip them.
					n.DecodeNativeHistogramStats = false
					break pathLoop
				}
			}
		}
	})
	return plan, nil
}
