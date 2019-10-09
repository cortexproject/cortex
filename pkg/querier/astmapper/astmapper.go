package astmapper

import (
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql"
)

// ASTMapper is the exported interface for mapping between multiple AST representations
type ASTMapper interface {
	Map(node promql.Node) (promql.Node, error)
}

type MapperFunc func(node promql.Node) (promql.Node, error)

func (fn MapperFunc) Map(node promql.Node) (promql.Node, error) {
	return fn(node)
}

type MultiMapper struct {
	mappers []ASTMapper
}

func (m *MultiMapper) Map(node promql.Node) (promql.Node, error) {
	var result promql.Node = node
	var err error

	if len(m.mappers) == 0 {
		return nil, errors.New("MultiMapper: No mappers registered")
	}

	for _, x := range m.mappers {
		result, err = x.Map(result)
		if err != nil {
			return nil, err
		}
	}
	return result, nil

}

// since registered functions are applied in the order they're registered, it's advised to register them
// in decreasing priority and only operate on nodes that each function cares about, defaulting to CloneNode.
func (m *MultiMapper) Register(xs ...ASTMapper) {
	m.mappers = append(m.mappers, xs...)
}

func NewMultiMapper(xs ...ASTMapper) *MultiMapper {
	m := &MultiMapper{}
	m.Register(xs...)
	return m
}

// helper function to clone a node.
func CloneNode(node promql.Node) (promql.Node, error) {
	return promql.ParseExpr(node.String())
}
