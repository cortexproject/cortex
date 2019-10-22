package astmapper

import (
	"fmt"

	"regexp"

	"strings"

	"strconv"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
)

const (
	// DefaultShards factor to assume
	DefaultShards = 12
	// ShardLabel is a reserved label referencing a cortex shard
	ShardLabel = "__cortex_shard__"
	// ShardLabelFmt is the fmt of the ShardLabel key.
	ShardLabelFmt = "%d_of_%d"
)

var (
	// ShardLabelRE matches a value in ShardLabelFmt
	ShardLabelRE = regexp.MustCompile("^[0-9]+_of_[0-9]+$")
)

type squasher = func(promql.Node) (promql.Expr, error)

type shardSummer struct {
	shards   int
	curshard *int
	squash   squasher
}

// NewShardSummer instantiates an ASTMapper which will fan out sums queries by shard
func NewShardSummer(shards int, squasher squasher) ASTMapper {
	if shards == 0 {
		shards = DefaultShards
	}

	return NewASTNodeMapper(&shardSummer{
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
func (summer *shardSummer) MapNode(node promql.Node) (promql.Node, bool, error) {

	switch n := node.(type) {
	case *promql.AggregateExpr:
		if CanParallel(n) {
			result, err := summer.shardSum(n)
			return result, true, err
		}

		return n, false, nil

	case *promql.VectorSelector:
		if summer.curshard != nil {
			mapped, err := shardVectorSelector(*summer.curshard, summer.shards, n)
			return mapped, true, err
		}
		return n, true, nil

	case *promql.MatrixSelector:
		if summer.curshard != nil {
			mapped, err := shardMatrixSelector(*summer.curshard, summer.shards, n)
			return mapped, true, err
		}
		return n, true, nil

	default:
		return n, false, nil
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

		subSummer := NewASTNodeMapper(summer.CopyWithCurshard(i))
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
	shardMatcher, err := labels.NewMatcher(labels.MatchEqual, ShardLabel, fmt.Sprintf(ShardLabelFmt, curshard, shards))
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
	shardMatcher, err := labels.NewMatcher(labels.MatchEqual, ShardLabel, fmt.Sprintf(ShardLabelFmt, curshard, shards))
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

// ParseShard will extract the shard information encoded in ShardLabelFmt
func ParseShard(input string) (x, of int, err error) {
	if !ShardLabelRE.MatchString(input) {
		return 0, 0, errors.Errorf("Invalid ShardLabel value: [%s]", input)
	}

	matches := strings.Split(input, "_")
	x, err = strconv.Atoi(matches[0])
	if err != nil {
		return 0, 0, err
	}
	of, err = strconv.Atoi(matches[2])
	if err != nil {
		return 0, 0, err
	}

	if x >= of {
		return 0, 0, errors.Errorf("Shards out of bounds: [%d] >= [%d]", x, of)
	}
	return x, of, err
}
