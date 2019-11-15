package astmapper

import (
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
)

/*
Design:

The prometheus api package enforces a (*promql.Engine argument), making it infeasible to do lazy AST
evaluation and substitution from within this package.
This leaves the (storage.Queryable) interface as the remaining target for conducting application level sharding.

The main idea is to analyze the AST and determine which subtrees can be parallelized. With those in hand, the queries may
be remapped into vector or matrix selectors utilizing a reserved label containing the original query. These may then be parallelized in the storage implementation.
*/

const (
	// QueryLabel is a reserved label containing an embedded query
	QueryLabel = "__cortex_queries__"
	// EmbeddedQueryFlag is a reserved label (metric name) denoting an embedded query
	EmbeddedQueryFlag = "__embedded_queries__"
)

// EmbeddedQueries is a wrapper type for encoding queries
type EmbeddedQueries struct {
	Concat []string `json:"Concat"`
}

// JSONCodec is a Codec impl that uses JSON representations of EmbeddedQueries structs
var JSONCodec Codec = jsonCodec{}

type jsonCodec struct{}

func (c jsonCodec) Encode(queries []string) string {
	embedded := EmbeddedQueries{
		Concat: queries,
	}
	b, err := json.Marshal(embedded)

	if err != nil {
		panic(err)
	}

	return string(b)
}

func (c jsonCodec) Decode(encoded string) (queries []string, err error) {
	var embedded EmbeddedQueries
	err = json.Unmarshal([]byte(encoded), &embedded)
	if err != nil {
		return nil, err
	}

	return embedded.Concat, nil
}

// A Codec is responsible for encoding/decoding queries
type Codec interface {
	Encode([]string) string
	Decode(string) ([]string, error)
}

// Squash reduces an AST into a single vector or matrix query which can be hijacked by a Queryable impl.
func Squash(codec Codec, isMatrix bool, nodes ...promql.Node) (promql.Expr, error) {
	if codec == nil {
		return nil, errors.Errorf("nil Codec")
	}

	// concat OR legs
	var strs []string
	for _, node := range nodes {
		strs = append(strs, node.String())
	}

	encoded := codec.Encode(strs)

	embeddedQuery, err := labels.NewMatcher(labels.MatchEqual, QueryLabel, encoded)

	if err != nil {
		return nil, err
	}

	if isMatrix {
		return &promql.MatrixSelector{
			Name:          EmbeddedQueryFlag,
			Range:         time.Minute,
			LabelMatchers: []*labels.Matcher{embeddedQuery},
		}, nil
	}

	return &promql.VectorSelector{
		Name:          EmbeddedQueryFlag,
		LabelMatchers: []*labels.Matcher{embeddedQuery},
	}, nil
}

// VectorSquasher always uses a VectorSelector as the substitution node.
// This is important because logical/set binops can only be applied against vectors and not matrices.
func VectorSquasher(nodes ...promql.Node) (promql.Expr, error) {
	return Squash(JSONCodec, false, nodes...)
}

// ShallowEmbedSelectors encodes selector queries if they do not already have the EmbeddedQueryFlag.
// This is primarily useful for deferring query execution.
var ShallowEmbedSelectors = NewASTNodeMapper(NodeMapperFunc(shallowEmbedSelectors))

func shallowEmbedSelectors(node promql.Node) (mapped promql.Node, finished bool, err error) {
	switch n := node.(type) {
	case *promql.VectorSelector:
		if n.Name == EmbeddedQueryFlag {
			return n, true, nil
		}
		squashed, err := Squash(JSONCodec, false, n)
		return squashed, true, err

	case *promql.MatrixSelector:
		if n.Name == EmbeddedQueryFlag {
			return n, true, nil
		}
		squashed, err := Squash(JSONCodec, true, n)
		return squashed, true, err

	default:
		return n, false, nil
	}
}
