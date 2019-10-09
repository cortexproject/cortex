package astmapper

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
)

const (
	DEFAULT_SHARDS = 12
	SHARD_LABEL    = "__cortex_shard__"
)

type squasher = func(promql.Node) (promql.Expr, error)

type ShardSummer struct {
	shards int
	squash squasher
}

func NewShardSummer(shards int, squasher func(promql.Node) (promql.Expr, error)) *ShardSummer {
	if shards == 0 {
		shards = DEFAULT_SHARDS
	}

	return &ShardSummer{shards, squasher}
}

// a MapperFunc adapter
func ShardSummerFunc(shards int, squash squasher) MapperFunc {
	summer := NewShardSummer(shards, squash)

	return summer.Map
}

// ShardSummer expands a query AST by sharding and re-summing when possible
func (summer *ShardSummer) Map(node promql.Node) (promql.Node, error) {
	return summer.mapWithOpts(MappingOpts{}, node)
}

type MappingOpts struct {
	isParallel bool
	// curShard's ptr denotes existence. This helps prevent selectors in non-sum queries from sharding themselves as they'll always register true for isParallel.
	curShard *int
}

// mapWithOpts carries information about whether the node is in a subtree that is a parallelism candidate.
func (summer *ShardSummer) mapWithOpts(opts MappingOpts, node promql.Node) (promql.Node, error) {
	// since mapWithOpts is called recursively, the new subtree may be parallelizable even if its parent is not
	opts.isParallel = opts.isParallel || CanParallel(node)

	switch n := node.(type) {
	case promql.Expressions:
		for i, e := range n {
			if mapped, err := summer.mapWithOpts(opts, e); err != nil {
				return nil, err
			} else {
				n[i] = mapped.(promql.Expr)
			}
		}
		return n, nil

	case *promql.AggregateExpr:
		if opts.isParallel {
			return summer.shardSum(n)
		}

		if mapped, err := summer.mapWithOpts(opts, n.Expr); err != nil {
			return nil, err
		} else {
			n.Expr = mapped.(promql.Expr)
		}
		return n, nil

	case *promql.BinaryExpr:
		if lhs, err := summer.mapWithOpts(opts, n.LHS); err != nil {
			return nil, err
		} else {
			n.LHS = lhs.(promql.Expr)
		}

		if rhs, err := summer.mapWithOpts(opts, n.RHS); err != nil {
			return nil, err
		} else {
			n.RHS = rhs.(promql.Expr)
		}
		return n, nil

	case *promql.Call:
		for i, e := range n.Args {
			if mapped, err := summer.mapWithOpts(opts, e); err != nil {
				return nil, err
			} else {
				n.Args[i] = mapped.(promql.Expr)
			}
		}
		return n, nil

	case *promql.SubqueryExpr:
		if mapped, err := summer.mapWithOpts(opts, n.Expr); err != nil {
			return nil, err
		} else {
			n.Expr = mapped.(promql.Expr)
		}
		return n, nil

	case *promql.ParenExpr:
		if mapped, err := summer.mapWithOpts(opts, n.Expr); err != nil {
			return nil, err
		} else {
			n.Expr = mapped.(promql.Expr)
		}
		return n, nil

	case *promql.UnaryExpr:
		if mapped, err := summer.mapWithOpts(opts, n.Expr); err != nil {
			return nil, err
		} else {
			n.Expr = mapped.(promql.Expr)
		}
		return n, nil

	case *promql.EvalStmt:
		if mapped, err := summer.mapWithOpts(opts, n.Expr); err != nil {
			return nil, err
		} else {
			n.Expr = mapped.(promql.Expr)
		}
		return n, nil

	case *promql.NumberLiteral, *promql.StringLiteral:
		return n, nil

	case *promql.VectorSelector:
		if opts.isParallel && opts.curShard != nil {
			return shardVectorSelector(*opts.curShard, summer.shards, n)
		} else {
			return n, nil
		}

	case *promql.MatrixSelector:
		if opts.isParallel && opts.curShard != nil {
			return shardMatrixSelector(*opts.curShard, summer.shards, n)
		} else {
			return n, nil
		}

	default:
		panic(errors.Errorf("ShardSummer: unhandled node type %T", node))
	}
}

func (summer *ShardSummer) shardSum(expr *promql.AggregateExpr) (promql.Node, error) {

	if summer.shards < 2 {
		return expr, nil
	}

	subSums := make([]promql.Expr, 0, summer.shards)

	for i := 0; i < summer.shards; i++ {
		cloned, err := CloneNode(expr.Expr)
		if err != nil {
			return nil, err
		}

		sharded, err := summer.mapWithOpts(MappingOpts{
			curShard: &i,
		}, cloned)
		if err != nil {
			return nil, err
		}

		var subSum promql.Expr = &promql.AggregateExpr{
			Op:       expr.Op,
			Expr:     sharded.(promql.Expr),
			Param:    expr.Param,
			Grouping: expr.Grouping,
			Without:  expr.Without,
		}

		if summer.squash != nil {
			subSum, err = summer.squash(subSum)
		}

		if err != nil {
			return nil, err
		}

		subSums = append(subSums,
			subSum,
		)

	}

	var combinedSums promql.Expr = subSums[0]
	for i := 1; i < len(subSums); i++ {
		combinedSums = &promql.BinaryExpr{
			Op:  promql.ItemLOR,
			LHS: combinedSums,
			RHS: subSums[i],
		}
	}

	return &promql.AggregateExpr{
			Op:       expr.Op,
			Expr:     combinedSums,
			Param:    expr.Param,
			Grouping: expr.Grouping,
			Without:  expr.Without,
		},
		nil
}

func shardVectorSelector(curshard, shards int, selector *promql.VectorSelector) (promql.Node, error) {
	shardMatcher, err := labels.NewMatcher(labels.MatchEqual, SHARD_LABEL, fmt.Sprintf("%d_of_%d", curshard, shards))
	if err != nil {
		return nil, err
	}

	return &promql.VectorSelector{
		Name:   selector.Name,
		Offset: selector.Offset,
		LabelMatchers: append(
			[]*labels.Matcher{shardMatcher},
			selector.LabelMatchers...,
		),
	}, nil
}

func shardMatrixSelector(curshard, shards int, selector *promql.MatrixSelector) (promql.Node, error) {
	shardMatcher, err := labels.NewMatcher(labels.MatchEqual, SHARD_LABEL, fmt.Sprintf("%d_of_%d", curshard, shards))
	if err != nil {
		return nil, err
	}

	return &promql.MatrixSelector{
		Name:   selector.Name,
		Range:  selector.Range,
		Offset: selector.Offset,
		LabelMatchers: append(
			[]*labels.Matcher{shardMatcher},
			selector.LabelMatchers...,
		),
	}, nil
}
