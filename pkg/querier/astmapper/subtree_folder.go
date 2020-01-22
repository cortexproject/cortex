package astmapper

import (
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql"
)

/*
subtreeFolder is a NodeMapper which embeds an entire promql.Node in an embedded query
if it does not contain any previously embedded queries. This allows the frontend to "zip up" entire
subtrees of an AST that have not already been parallelized.

*/
type subtreeFolder struct {
	codec Codec
}

// NewSubtreeFolder creates a subtreeFolder with a specified codec
func NewSubtreeFolder(codec Codec) (ASTMapper, error) {
	if codec == nil {
		return nil, errors.New("nil codec")
	}
	return NewASTNodeMapper(&subtreeFolder{
		codec: JSONCodec,
	}), nil
}

// MapNode impls NodeMapper
func (f *subtreeFolder) MapNode(node promql.Node) (promql.Node, bool, error) {
	switch n := node.(type) {
	// do not attempt to fold number or string leaf nodes
	case *promql.NumberLiteral, *promql.StringLiteral:
		return n, true, nil
	}

	containsEmbedded, err := Predicate(node, predicate(isEmbedded))
	if err != nil {
		return nil, true, err
	}

	if containsEmbedded {
		return node, false, nil
	}

	expr, err := VectorSquasher(node)
	return expr, true, err
}

func isEmbedded(node promql.Node) (bool, error) {
	switch n := node.(type) {
	case *promql.VectorSelector:
		if n.Name == EmbeddedQueryFlag {
			return true, nil
		}

	case *promql.MatrixSelector:
		if n.Name == EmbeddedQueryFlag {
			return true, nil
		}

	}
	return false, nil
}

type predicate = func(promql.Node) (bool, error)

// Predicate is a helper which uses promql.Walk under the hood determine if any node in a subtree
// returns true for a specified function
func Predicate(node promql.Node, fn predicate) (bool, error) {
	v := &visitor{
		fn: fn,
	}

	if err := promql.Walk(v, node, nil); err != nil {
		return false, err
	}
	return v.result, nil
}

type visitor struct {
	fn     predicate
	result bool
}

// Visit impls promql.Visitor
func (v *visitor) Visit(node promql.Node, path []promql.Node) (promql.Visitor, error) {
	// if the visitor has already seen a predicate success, don't overwrite
	if v.result {
		return nil, nil
	}

	var err error

	v.result, err = v.fn(node)
	if err != nil {
		return nil, err
	}
	if v.result {
		return nil, nil
	}
	return v, nil
}
