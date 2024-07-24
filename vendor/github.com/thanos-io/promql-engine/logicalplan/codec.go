// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"bytes"
	"encoding/json"
	"math"

	"github.com/prometheus/prometheus/model/labels"
)

const (
	nanVal    = `"NaN"`
	infVal    = `"+Inf"`
	negInfVal = `"-Inf"`
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
	var data json.RawMessage = nil
	// Special handling for -Inf/+Inf values.
	if n, ok := node.(*NumberLiteral); ok {
		if math.IsInf(n.Val, 1) {
			data = json.RawMessage(infVal)
		}
		if math.IsInf(n.Val, -1) {
			data = json.RawMessage(negInfVal)
		}
		if math.IsNaN(n.Val) {
			data = json.RawMessage(nanVal)
		}
	}
	if data == nil {
		var err error
		data, err = json.Marshal(node)
		if err != nil {
			return nil, err
		}
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
		var err error
		for i, m := range v.LabelMatchers {
			v.LabelMatchers[i], err = labels.NewMatcher(m.Type, m.Name, m.Value)
			if err != nil {
				return nil, err
			}
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
		if bytes.Equal(t.Data, []byte(infVal)) {
			n.Val = math.Inf(1)
		} else if bytes.Equal(t.Data, []byte(negInfVal)) {
			n.Val = math.Inf(-1)
		} else if bytes.Equal(t.Data, []byte(nanVal)) {
			n.Val = math.NaN()
		} else {
			if err := json.Unmarshal(t.Data, n); err != nil {
				return nil, err
			}
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
