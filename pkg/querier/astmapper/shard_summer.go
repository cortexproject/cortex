package astmapper

import (
	"fmt"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
)

const (
	DEFAULT_SHARDS = 12
	SHARD_LABEL    = "__cortex_shard__"
)

type squasher = func(promql.Node) (promql.Expr, error)

type shardSummer struct {
	shards   int
	curshard *int
	squash   squasher
}

func NewShardSummer(shards int, squasher squasher) ASTMapper {
	if shards == 0 {
		shards = DEFAULT_SHARDS
	}

	return NewNodeMapper(&shardSummer{
		shards:   shards,
		squash:   squasher,
		curshard: nil,
	})
}

// CopyWithCurshard clones a shardSummer with a new current shard. This facilitates recursive sharding.
func (summer *shardSummer) CopyWithCurshard(curshard int) *shardSummer {
	s := *summer
	s.curshard = &curshard
	return &s
}

// shardSummer expands a query AST by sharding and re-summing when possible
func (summer *shardSummer) MapNode(node promql.Node) (promql.Node, error, bool) {

	switch n := node.(type) {
	case *promql.AggregateExpr:
		if CanParallel(n) {
			result, err := summer.shardSum(n)
			return result, err, true
		}

		return n, nil, false

	case *promql.VectorSelector:
		if summer.curshard != nil {
			mapped, err := shardVectorSelector(*summer.curshard, summer.shards, n)
			return mapped, err, true
		} else {
			return n, nil, true
		}

	case *promql.MatrixSelector:
		if summer.curshard != nil {
			mapped, err := shardMatrixSelector(*summer.curshard, summer.shards, n)
			return mapped, err, true
		} else {
			return n, nil, true
		}

	default:
		return n, nil, false
	}
}

// shardSum contains the logic for how we split/stitch legs of a parallelized query
func (summer *shardSummer) shardSum(expr *promql.AggregateExpr) (promql.Node, error) {

	if summer.shards < 2 {
		return expr, nil
	}

	subSums := make([]promql.Expr, 0, summer.shards)

	for i := 0; i < summer.shards; i++ {
		cloned, err := CloneNode(expr.Expr)
		if err != nil {
			return nil, err
		}

		subSummer := NewNodeMapper(summer.CopyWithCurshard(i))
		sharded, err := subSummer.Map(cloned)
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
