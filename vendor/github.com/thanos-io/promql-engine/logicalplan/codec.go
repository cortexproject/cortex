// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"encoding/json"
)

type jsonNode struct {
	Type     NodeType          `json:"type"`
	Data     json.RawMessage   `json:"data"`
	Children []json.RawMessage `json:"children,omitempty"`
}

func Marshal(node Node) ([]byte, error) {
	clone := node.Clone()
	return marshalNode(clone)
}

func marshalNode(node Node) ([]byte, error) {
	children := make([]json.RawMessage, 0, len(node.Children()))
	for _, c := range node.Children() {
		childData, err := marshalNode(*c)
		if err != nil {
			return nil, err
		}
		children = append(children, childData)
	}
	data, err := json.Marshal(node)
	if err != nil {
		return nil, err
	}
	return json.Marshal(jsonNode{
		Type:     node.Type(),
		Data:     data,
		Children: children,
	})
}

func Unmarshal(data []byte) (Node, error) {
	return unmarshalNode(data)
}

func unmarshalNode(data []byte) (Node, error) {
	t := jsonNode{}
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, err
	}

	switch t.Type {
	case VectorSelectorNode:
		v := &VectorSelector{}
		if err := json.Unmarshal(t.Data, v); err != nil {
			return nil, err
		}
		return v, nil
	case MatrixSelectorNode:
		m := &MatrixSelector{}
		if err := json.Unmarshal(t.Data, m); err != nil {
			return nil, err
		}
		vs, err := unmarshalNode(t.Children[0])
		if err != nil {
			return nil, err
		}
		m.VectorSelector = vs.(*VectorSelector)
		return m, nil
	case AggregationNode:
		a := &Aggregation{}
		if err := json.Unmarshal(t.Data, a); err != nil {
			return nil, err
		}
		var err error
		a.Expr, err = unmarshalNode(t.Children[0])
		if err != nil {
			return nil, err
		}
		if len(t.Children) > 1 {
			a.Param, err = unmarshalNode(t.Children[1])
			if err != nil {
				return nil, err
			}
		}
		return a, nil
	case BinaryNode:
		b := &Binary{}
		if err := json.Unmarshal(t.Data, b); err != nil {
			return nil, err
		}
		var err error
		b.LHS, err = unmarshalNode(t.Children[0])
		if err != nil {
			return nil, err
		}
		b.RHS, err = unmarshalNode(t.Children[1])
		if err != nil {
			return nil, err
		}
		return b, nil
	case FunctionNode:
		f := &FunctionCall{}
		if err := json.Unmarshal(t.Data, f); err != nil {
			return nil, err
		}
		for _, c := range t.Children {
			child, err := unmarshalNode(c)
			if err != nil {
				return nil, err
			}
			f.Args = append(f.Args, child)
		}
		return f, nil
	case NumberLiteralNode:
		n := &NumberLiteral{}
		if err := json.Unmarshal(t.Data, n); err != nil {
			return nil, err
		}
		return n, nil
	case StringLiteralNode:
		s := &StringLiteral{}
		if err := json.Unmarshal(t.Data, s); err != nil {
			return nil, err
		}
		return s, nil
	case SubqueryNode:
		s := &Subquery{}
		if err := json.Unmarshal(t.Data, s); err != nil {
			return nil, err
		}
		var err error
		s.Expr, err = unmarshalNode(t.Children[0])
		if err != nil {
			return nil, err
		}
		return s, nil
	case CheckDuplicateNode:
		c := &CheckDuplicateLabels{}
		if err := json.Unmarshal(t.Data, c); err != nil {
			return nil, err
		}
		var err error
		c.Expr, err = unmarshalNode(t.Children[0])
		if err != nil {
			return nil, err
		}
		return c, nil
	case StepInvariantNode:
		s := &StepInvariantExpr{}
		if err := json.Unmarshal(t.Data, s); err != nil {
			return nil, err
		}
		var err error
		s.Expr, err = unmarshalNode(t.Children[0])
		if err != nil {
			return nil, err
		}
		return s, nil
	case ParensNode:
		p := &Parens{}
		if err := json.Unmarshal(t.Data, p); err != nil {
			return nil, err
		}
		var err error
		p.Expr, err = unmarshalNode(t.Children[0])
		if err != nil {
			return nil, err
		}
		return p, nil
	case UnaryNode:
		u := &Unary{}
		if err := json.Unmarshal(t.Data, u); err != nil {
			return nil, err
		}
		var err error
		u.Expr, err = unmarshalNode(t.Children[0])
		if err != nil {
			return nil, err
		}
		return u, nil
	}
	return nil, nil
}
