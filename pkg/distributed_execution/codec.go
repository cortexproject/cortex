package distributed_execution

import (
	"bytes"
	"encoding/json"
	"math"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/promql-engine/logicalplan"
)

type jsonNode struct {
	Type     logicalplan.NodeType `json:"type"`
	Data     json.RawMessage      `json:"data"`
	Children []json.RawMessage    `json:"children,omitempty"`
}

const (
	nanVal    = `"NaN"`
	infVal    = `"+Inf"`
	negInfVal = `"-Inf"`
)

func Unmarshal(data []byte) (logicalplan.Node, error) {
	return unmarshalNode(data)
}

func unmarshalNode(data []byte) (logicalplan.Node, error) {
	t := jsonNode{}
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, err
	}

	switch t.Type {
	case logicalplan.VectorSelectorNode:
		v := &logicalplan.VectorSelector{}
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
	case logicalplan.MatrixSelectorNode:
		m := &logicalplan.MatrixSelector{}
		if err := json.Unmarshal(t.Data, m); err != nil {
			return nil, err
		}
		vs, err := unmarshalNode(t.Children[0])
		if err != nil {
			return nil, err
		}
		m.VectorSelector = vs.(*logicalplan.VectorSelector)
		return m, nil
	case logicalplan.AggregationNode:
		a := &logicalplan.Aggregation{}
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
	case logicalplan.BinaryNode:
		b := &logicalplan.Binary{}
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
	case logicalplan.FunctionNode:
		f := &logicalplan.FunctionCall{}
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
	case logicalplan.NumberLiteralNode:
		n := &logicalplan.NumberLiteral{}
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
	case logicalplan.StringLiteralNode:
		s := &logicalplan.StringLiteral{}
		if err := json.Unmarshal(t.Data, s); err != nil {
			return nil, err
		}
		return s, nil
	case logicalplan.SubqueryNode:
		s := &logicalplan.Subquery{}
		if err := json.Unmarshal(t.Data, s); err != nil {
			return nil, err
		}
		var err error
		s.Expr, err = unmarshalNode(t.Children[0])
		if err != nil {
			return nil, err
		}
		return s, nil
	case logicalplan.CheckDuplicateNode:
		c := &logicalplan.CheckDuplicateLabels{}
		if err := json.Unmarshal(t.Data, c); err != nil {
			return nil, err
		}
		var err error
		c.Expr, err = unmarshalNode(t.Children[0])
		if err != nil {
			return nil, err
		}
		return c, nil
	case logicalplan.StepInvariantNode:
		s := &logicalplan.StepInvariantExpr{}
		if err := json.Unmarshal(t.Data, s); err != nil {
			return nil, err
		}
		var err error
		s.Expr, err = unmarshalNode(t.Children[0])
		if err != nil {
			return nil, err
		}
		return s, nil
	case logicalplan.ParensNode:
		p := &logicalplan.Parens{}
		if err := json.Unmarshal(t.Data, p); err != nil {
			return nil, err
		}
		var err error
		p.Expr, err = unmarshalNode(t.Children[0])
		if err != nil {
			return nil, err
		}
		return p, nil
	case logicalplan.UnaryNode:
		u := &logicalplan.Unary{}
		if err := json.Unmarshal(t.Data, u); err != nil {
			return nil, err
		}
		var err error
		u.Expr, err = unmarshalNode(t.Children[0])
		if err != nil {
			return nil, err
		}
		return u, nil
	case RemoteNode:
		r := &Remote{}
		if err := json.Unmarshal(t.Data, r); err != nil {
			return nil, err
		}
		var err error
		r.Expr, err = unmarshalNode(t.Children[0])
		if err != nil {
			return nil, err
		}
		return r, nil
	}
	return nil, nil
}
