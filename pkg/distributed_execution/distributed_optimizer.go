package distributed_execution

import (
	"fmt"

	"github.com/prometheus/prometheus/util/annotations"
	"github.com/thanos-io/promql-engine/logicalplan"
)

// This is a simplified implementation that only handles binary aggregation cases
// Future versions of the distributed optimizer are expected to:
// - Support more complex query patterns
// - Incorporate diverse optimization strategies
// - Extend support to node types beyond binary operations

type DistributedOptimizer struct{}

func (d *DistributedOptimizer) Optimize(root logicalplan.Node) (logicalplan.Node, annotations.Annotations, error) {
	warns := annotations.New()

	if root == nil {
		return nil, *warns, fmt.Errorf("nil root node")
	}

	var hasAggregation bool
	logicalplan.TraverseBottomUp(nil, &root, func(parent, current *logicalplan.Node) bool {

		if (*current).Type() == logicalplan.AggregationNode {
			hasAggregation = true
		}

		if (*current).Type() == logicalplan.BinaryNode && hasAggregation {
			ch := (*current).Children()

			for _, child := range ch {
				temp := (*child).Clone()
				*child = &Remote{}
				*(*child).Children()[0] = temp
			}

			hasAggregation = false
		}

		return false
	})
	return root, *warns, nil
}
