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

// a NodeMapper either maps a single AST node or returns the unaltered node.
// It also returns a bool to signal that no further recursion is necessary.
// This is helpful because it allows mappers to only implement logic for node types they want to change.
// It makes some mappers trivially easy to implement (see ShallowEmbedSelectors from embedded.go)
type NodeMapper interface {
	MapNode(node promql.Node) (mapped promql.Node, err error, finished bool)
}

type NodeMapperFunc func(node promql.Node) (promql.Node, error, bool)

func (f NodeMapperFunc) MapNode(node promql.Node) (promql.Node, error, bool) {
	return f(node)
}

func NewNodeMapper(mapper NodeMapper) *nodeMapper {
	return &nodeMapper{mapper}
}

// nodeMapper is an ASTMapper adapter which uses a NodeMapper internally.
type nodeMapper struct {
	NodeMapper
}

func (nm *nodeMapper) Map(node promql.Node) (promql.Node, error) {
	node, err, fin := nm.MapNode(node)

	if err != nil {
		return nil, err
	}

	if fin {
		return node, nil
	}

	switch n := node.(type) {
	case nil:
		// nil handles cases where we check optional fields that are not set
		return nil, nil

	case promql.Expressions:
		for i, e := range n {
			if mapped, err := nm.Map(e); err != nil {
				return nil, err
			} else {
				n[i] = mapped.(promql.Expr)
			}
		}
		return n, nil

	case *promql.AggregateExpr:
		expr, err := nm.Map(n.Expr)
		if err != nil {
			return nil, err
		}
		n.Expr = expr.(promql.Expr)
		return n, nil

	case *promql.BinaryExpr:
		if lhs, err := nm.Map(n.LHS); err != nil {
			return nil, err
		} else {
			n.LHS = lhs.(promql.Expr)
		}

		if rhs, err := nm.Map(n.RHS); err != nil {
			return nil, err
		} else {
			n.RHS = rhs.(promql.Expr)
		}
		return n, nil

	case *promql.Call:
		for i, e := range n.Args {
			if mapped, err := nm.Map(e); err != nil {
				return nil, err
			} else {
				n.Args[i] = mapped.(promql.Expr)
			}
		}
		return n, nil

	case *promql.SubqueryExpr:
		if mapped, err := nm.Map(n.Expr); err != nil {
			return nil, err
		} else {
			n.Expr = mapped.(promql.Expr)
		}
		return n, nil

	case *promql.ParenExpr:
		if mapped, err := nm.Map(n.Expr); err != nil {
			return nil, err
		} else {
			n.Expr = mapped.(promql.Expr)
		}
		return n, nil

	case *promql.UnaryExpr:
		if mapped, err := nm.Map(n.Expr); err != nil {
			return nil, err
		} else {
			n.Expr = mapped.(promql.Expr)
		}
		return n, nil

	case *promql.EvalStmt:
		if mapped, err := nm.Map(n.Expr); err != nil {
			return nil, err
		} else {
			n.Expr = mapped.(promql.Expr)
		}
		return n, nil

	case *promql.NumberLiteral, *promql.StringLiteral, *promql.VectorSelector, *promql.MatrixSelector:
		return n, nil

	default:
		panic(errors.Errorf("nodeMapper: unhandled node type %T", node))
	}
}
