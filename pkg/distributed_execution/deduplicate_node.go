package distributed_execution

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/promql-engine/execution"
	"github.com/thanos-io/promql-engine/execution/exchange"
	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
)

const (
	ShardedRemoteExecutionNode logicalplan.NodeType = "ShardedRemoteExecutionNode"
)

var _ logicalplan.UserDefinedExpr = (*ShardedRemoteExecutions)(nil)

// ShardedRemoteExecutions is a custom logical-plan node that fans a single
// aggregation out into one sub-expression per shard. At execution time each
// sub-expression is built into its own operator and the partial results are
// coalesced together, so a parent aggregation (e.g. sum) can combine them.
//
// NOTE: This is experimental. It is reconciled to compile against the current
// Thanos engine API, but it is not yet wired end-to-end (see caveats in the
// port notes): the node has no JSON (un)marshaling for codec round-tripping,
// and the shard matcher injected by the optimizer does not match the merged
// metric-name-hash storage sharding mechanism.
type ShardedRemoteExecutions struct {
	Expressions []logicalplan.Node `json:"-"`
}

// MakeExecutionOperator builds one operator per shard sub-expression and
// coalesces them into a single operator stream for the parent aggregation.
func (r *ShardedRemoteExecutions) MakeExecutionOperator(
	ctx context.Context,
	opts *query.Options,
	hints storage.SelectHints,
) (model.VectorOperator, error) {
	operators := make([]model.VectorOperator, len(r.Expressions))
	var err error
	for i := range operators {
		operators[i], err = execution.New(ctx, r.Expressions[i], nil, opts)
		if err != nil {
			return nil, err
		}
	}
	coalesce := exchange.NewCoalesce(opts, 0, operators...)
	return exchange.NewConcurrent(coalesce, 2, opts), nil
}

func (r *ShardedRemoteExecutions) Clone() logicalplan.Node {
	clone := &ShardedRemoteExecutions{Expressions: make([]logicalplan.Node, len(r.Expressions))}
	for i, e := range r.Expressions {
		clone.Expressions[i] = e.Clone()
	}
	return clone
}

func (r *ShardedRemoteExecutions) Children() []*logicalplan.Node {
	children := make([]*logicalplan.Node, len(r.Expressions))
	for i := range r.Expressions {
		children[i] = &r.Expressions[i]
	}
	return children
}

func (r *ShardedRemoteExecutions) String() string {
	return fmt.Sprintf("shard(%d)", len(r.Expressions))
}

func (r *ShardedRemoteExecutions) ReturnType() parser.ValueType { return parser.ValueTypeVector }

func (r *ShardedRemoteExecutions) Type() logicalplan.NodeType { return ShardedRemoteExecutionNode }
