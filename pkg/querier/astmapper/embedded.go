package astmapper

import (
	"encoding/hex"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"time"
)

/*
Design:

Unfortunately. the prometheus api package enforces a (*promql.Engine argument), making it infeasible to do lazy AST
evaluation and substitution from within this package.
This leaves the (storage.Queryable) interface as the remaining target for conducting application level sharding.

The main idea is to analyze the AST and determine which subtrees can be parallelized. With those in hand, the queries may
be remapped into vector or matrix selectors utilizing a reserved label containing the original query.

These may then be parallelized in the storage impl.

Ideally the promql.Engine could be an interface instead of a concrete type, allowing us to conduct all parallelism from within the AST via the Engine and pass retrieval requests to the storage.Queryable ifc.
*/

const (
	QUERY_LABEL         = "__cortex_query__"
	EMBEDDED_QUERY_FLAG = "__embedded_query__"
)

// Squash reduces an AST into a single vector or matrix query which can be hijacked by a Queryable impl. The important part is that return types align.
// TODO(owen): handle inferring return types from different functions/operators
func Squash(node promql.Node, isMatrix bool) (promql.Expr, error) {
	// promql's label charset is not a subset of promql's syntax charset. Therefor we use hex as an intermediary
	encoded := hex.EncodeToString([]byte(node.String()))

	embedded_query, err := labels.NewMatcher(labels.MatchEqual, QUERY_LABEL, encoded)

	if err != nil {
		return nil, err
	}

	if isMatrix {
		return &promql.MatrixSelector{
			Name:          EMBEDDED_QUERY_FLAG,
			Range:         time.Minute,
			LabelMatchers: []*labels.Matcher{embedded_query},
		}, nil
	} else {
		return &promql.VectorSelector{
			Name:          EMBEDDED_QUERY_FLAG,
			LabelMatchers: []*labels.Matcher{embedded_query},
		}, nil
	}
}

// VectorSquasher always uses a VectorSelector as the substitution node.
// This is important because logical/set binops can only be applied against vectors and not matrices.
func VectorSquasher(node promql.Node) (promql.Expr, error) {
	return Squash(node, false)
}

// ShallowEmbedSelectors encodes selector queries if they do not already have the EMBEDDED_QUERY_FLAG.
// This is primarily useful for deferring query execution.
var ShallowEmbedSelectors = &NodeMapper{MapperFunc(shallowEmbedSelectors)}

func shallowEmbedSelectors(node promql.Node) (promql.Node, error) {

	switch n := node.(type) {
	case *promql.VectorSelector:
		if n.Name == EMBEDDED_QUERY_FLAG {
			return n, nil
		}
		return Squash(n, false)

	case *promql.MatrixSelector:
		if n.Name == EMBEDDED_QUERY_FLAG {
			return n, nil
		}
		return Squash(n, true)

	default:
		return n, nil
	}
}
