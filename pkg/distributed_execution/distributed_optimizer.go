package distributed_execution

import (
	"github.com/thanos-io/promql-engine/query"

	"github.com/prometheus/prometheus/util/annotations"
	"github.com/thanos-io/promql-engine/logicalplan"
)

// This is a simplified implementation that only handles binary aggregation cases
// Future versions of the distributed optimizer are expected to:
// - Support more complex query patterns
// - Incorporate diverse optimization strategies
// - Extend support to node types beyond binary operations

type DistributedOptimizer struct{}

func (d *DistributedOptimizer) Optimize(root logicalplan.Node, opts *query.Options) (logicalplan.Node, annotations.Annotations) {
	warns := annotations.New()

	logicalplan.TraverseBottomUp(nil, &root, func(parent, current *logicalplan.Node) bool {

		if (*current).Type() == logicalplan.BinaryNode && d.hasAggregation(current) {
			ch := (*current).Children()

			for _, child := range ch {
				temp := (*child).Clone()
				*child = NewRemoteNode(temp)
				*(*child).Children()[0] = temp
			}
		}

		return false
	})

	return root, *warns
}

func (d *DistributedOptimizer) hasAggregation(root *logicalplan.Node) bool {
	isAggr := false
	logicalplan.TraverseBottomUp(nil, root, func(parent, current *logicalplan.Node) bool {
		if (*current).Type() == logicalplan.AggregationNode {
			isAggr = true
			return true
		}
		return false
	})
	return isAggr
}
