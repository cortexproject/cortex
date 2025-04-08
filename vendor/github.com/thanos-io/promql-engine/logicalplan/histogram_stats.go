// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"github.com/thanos-io/promql-engine/query"

	"github.com/prometheus/prometheus/util/annotations"
)

type DetectHistogramStatsOptimizer struct{}

func (d DetectHistogramStatsOptimizer) Optimize(plan Node, _ *query.Options) (Node, annotations.Annotations) {
	return d.optimize(plan, false)
}

func (d DetectHistogramStatsOptimizer) optimize(plan Node, decodeStats bool) (Node, annotations.Annotations) {
	var stop bool
	Traverse(&plan, func(node *Node) {
		if stop {
			return
		}
		switch n := (*node).(type) {
		case *VectorSelector:
			n.DecodeNativeHistogramStats = decodeStats
		case *FunctionCall:
			switch n.Func.Name {
			case "histogram_count", "histogram_sum", "histogram_avg":
				n.Args[0], _ = d.optimize(n.Args[0], true)
				stop = true
				return
			case "histogram_quantile":
				n.Args[1], _ = d.optimize(n.Args[1], false)
				stop = true
				return
			case "histogram_fraction":
				n.Args[2], _ = d.optimize(n.Args[2], false)
				stop = true
				return
			case "histogram_stddev", "histogram_stdvar":
				n.Args[0], _ = d.optimize(n.Args[0], false)
				stop = true
				return
			}
		}
	})
	return plan, nil
}
