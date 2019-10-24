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
		if CanParallel(n) && n.Op == promql.ItemSum {
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

	parent, subSums, err := summer.splitSum(expr)
	if err != nil {
		return nil, err
	}

	var combinedSums = subSums[0]
	for i := 1; i < len(subSums); i++ {
		combinedSums = &promql.BinaryExpr{
			Op:  promql.ItemLOR,
			LHS: combinedSums,
			RHS: subSums[i],
		}
	}
	parent.Expr = combinedSums
	return parent, nil
}

// splitSum takes a shardFactor and a sum expr and will form the new parent and child legs of a parallel variant
func (summer *shardSummer) splitSum(
	expr *promql.AggregateExpr,
) (
	parent *promql.AggregateExpr,
	children []promql.Expr,
	err error,
) {
	parent = &promql.AggregateExpr{
		Op:       expr.Op,
		Param:    expr.Param,
		Grouping: expr.Grouping,
		Without:  expr.Without,
	}
	var mkChild func(sharded *promql.AggregateExpr) promql.Expr

	if expr.Without {
		/*
			parallelizing a sum using without(foo) is representable as
			sum without(foo) (
			  sum without(__cortex_shard__) (rate(bar1{__cortex_shard__="0_of_2",baz="blip"}[1m])) or
			  sum without(__cortex_shard__) (rate(bar1{__cortex_shard__="1_of_2",baz="blip"}[1m]))
			)
		*/
		mkChild = func(sharded *promql.AggregateExpr) promql.Expr {
			sharded.Grouping = []string{ShardLabel}
			sharded.Without = true
			return sharded
		}
	} else if len(expr.Grouping) > 0 {
		/*
			parallelizing a sum using by(foo) is representable as
			sum by(foo) (
			  sum by(foo, __cortex_shard__) (rate(bar1{__cortex_shard__="0_of_3",baz="blip"}[1m])) or
			  sum by(foo, __cortex_shard__) (rate(bar1{__cortex_shard__="1_of_3",baz="blip"}[1m]))
			)
		*/
		mkChild = func(sharded *promql.AggregateExpr) promql.Expr {
			groups := make([]string, 0, len(expr.Grouping)+1)
			groups = append(groups, expr.Grouping...)
			groups = append(groups, ShardLabel)
			sharded.Grouping = groups
			return sharded
		}
	} else {
		/*
			parallelizing a non-parameterized sum is representable as
			sum(
			  sum without(__cortex_shard__) (rate(bar1{__cortex_shard__="0_of_3",baz="blip"}[1m])) or
			  sum without(__cortex_shard__) (rate(bar1{__cortex_shard__="1_of_3",baz="blip"}[1m]))
			)
		*/
		mkChild = func(sharded *promql.AggregateExpr) promql.Expr {
			sharded.Grouping = []string{ShardLabel}
			sharded.Without = true
			return sharded
		}
	}

	// iterate across shardFactor to create children
	for i := 0; i < summer.shards; i++ {
		cloned, err := CloneNode(expr.Expr)
		if err != nil {
			return parent, children, err
		}

		subSummer := NewASTNodeMapper(summer.CopyWithCurshard(i))
		sharded, err := subSummer.Map(cloned)
		if err != nil {
			return parent, children, err
		}

		subSum := mkChild(&promql.AggregateExpr{
			Op:   expr.Op,
			Expr: sharded.(promql.Expr),
		})

		if summer.squash != nil {
			subSum, err = summer.squash(subSum)
		}

		if err != nil {
			return parent, children, err
		}

		children = append(children,
			subSum,
		)
	}

	return parent, children, nil
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
func ParseShard(input string) (parsed ShardAnnotation, err error) {
	if !ShardLabelRE.MatchString(input) {
		return parsed, errors.Errorf("Invalid ShardLabel value: [%s]", input)
	}

	matches := strings.Split(input, "_")
	x, err := strconv.Atoi(matches[0])
	if err != nil {
		return parsed, err
	}
	of, err := strconv.Atoi(matches[2])
	if err != nil {
		return parsed, err
	}

	if x >= of {
		return parsed, errors.Errorf("Shards out of bounds: [%d] >= [%d]", x, of)
	}
	return ShardAnnotation{
		Shard: x,
		Of:    of,
	}, err
}

// ShardAnnotation is a convenience struct which holds data from a parsed shard label
type ShardAnnotation struct {
	Shard int
	Of    int
}

func (shard ShardAnnotation) String() string {
	return fmt.Sprintf(ShardLabelFmt, shard.Shard, shard.Of)
}

// ShardFromMatchers extracts a ShardAnnotation and the index it was pulled from in the matcher list
func ShardFromMatchers(matchers []*labels.Matcher) (shard *ShardAnnotation, idx int, err error) {
	for i, matcher := range matchers {
		if matcher.Type == labels.MatchEqual && matcher.Name == ShardLabel {
			shard, err := ParseShard(matcher.Value)
			if err != nil {
				return nil, i, err
			}
			return &shard, i, nil
		}
	}
	return nil, 0, nil
}
