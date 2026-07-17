package distributed_execution

import (
	"fmt"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
)

// This optimizer inserts Remote nodes so portions of the plan can be executed
// remotely. It supports two strategies:
//   - Binary-aggregation splitting: each operand of a binary expression that
//     contains an aggregation is offloaded to a Remote fragment.
//   - Sharded-aggregation splitting (experimental): a sum/count aggregation is
//     rewritten into sum(ShardedRemoteExecutions{per-shard sub-aggregations}),
//     so a single aggregation can be evaluated across shards and merged.
//
// NOTE: The sharded-aggregation path is experimental and not yet wired
// end-to-end (no codec round-tripping for ShardedRemoteExecutions, and the
// injected shard matcher does not match the merged metric-name-hash storage
// sharding). It is reconciled here only to compile and to allow local
// iteration.
type DistributedOptimizer struct {
	// ShardCount is the number of shards the sharded-aggregation strategy fans
	// an aggregation out into. It should match the compactor's per-tenant
	// metric-name-shard-size so per-shard subqueries route to disjoint blocks.
	ShardCount int
}

func (d *DistributedOptimizer) Optimize(root logicalplan.Node, opts *query.Options) (logicalplan.Node, annotations.Annotations) {
	warns := annotations.New()

	if root == nil {
		return root, *warns
	}

	// Strategy 1 (experimental): shard aggregations (sum/count) across shards.
	// Only runs when a positive shard count is configured; otherwise it is left
	// to the pre-existing binary-splitting strategy below.
	sharded := false
	if shardCount := d.ShardCount; shardCount > 0 {
		logicalplan.TraverseBottomUp(nil, &root, func(parent, current *logicalplan.Node) bool {
			if aggr, ok := (*current).(*logicalplan.Aggregation); ok {
				switch aggr.Op {
				case parser.SUM, parser.COUNT:
					subqueries := newRemoteAggregation(aggr, shardCount)
					*current = &logicalplan.Aggregation{
						Op:       parser.SUM,
						Expr:     &ShardedRemoteExecutions{Expressions: subqueries},
						Param:    aggr.Param,
						Grouping: aggr.Grouping,
						Without:  aggr.Without,
					}
					sharded = true
				}
				return true
			}
			return false
		})
	}

	// Strategy 2 (pre-existing): offload binary-expression operands that
	// contain an aggregation to Remote fragments. Skipped when the
	// aggregation-sharding pass already distributed the plan, to avoid
	// double-distributing the same subtrees.
	if !sharded {
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
	}

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

// newRemoteAggregation produces one Remote-wrapped per-shard copy of the given
// aggregation, tagging each copy's selectors with its shard identity.
func newRemoteAggregation(rootAggregation *logicalplan.Aggregation, shardNum int) []logicalplan.Node {
	nodes := make([]logicalplan.Node, 0, shardNum)
	for i := range shardNum {
		rc := rootAggregation.Expr.Clone()
		node := insertShardNum(&Remote{
			Expr: &logicalplan.Aggregation{
				Op:       rootAggregation.Op,
				Expr:     rc,
				Param:    rootAggregation.Param,
				Grouping: rootAggregation.Grouping,
				Without:  rootAggregation.Without,
			},
		}, shardNum, i)
		nodes = append(nodes, node)
	}
	return nodes
}

// insertShardNum tags every vector selector in the subtree with the shard it
// belongs to. NOTE: the label used here does not match the merged
// metric-name-hash storage sharding; this is experimental.
func insertShardNum(root logicalplan.Node, shardCount int, shardIdx int) logicalplan.Node {
	logicalplan.TraverseBottomUp(nil, &root, func(parent, current *logicalplan.Node) bool {
		if (*current).Type() == logicalplan.VectorSelectorNode {
			cur := (*current).(*logicalplan.VectorSelector)
			cur.LabelMatchers = append(cur.LabelMatchers, labels.MustNewMatcher(labels.MatchEqual, "__CORTEX_DQE_SHARD__", fmt.Sprintf("%d_%d", shardCount, shardIdx)))
		}
		return false
	})
	return root
}
