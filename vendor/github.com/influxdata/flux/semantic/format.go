package semantic

import (
	"fmt"
	"strings"
)

type FormatOption func(*formatter)

// Formatted produces a Formatter object suitable for printing
// using the standard fmt package.
// Currently only works for some expressions.
func Formatted(n Node, opts ...FormatOption) fmt.Formatter {
	f := formatter{
		n: n,
	}
	for _, o := range opts {
		o(&f)
	}
	return f
}

type formatter struct {
	n Node
}

func (f formatter) Format(fs fmt.State, c rune) {
	v := &formattingVisitor{}
	Walk(v, f.n)
	var result string
	if len(v.stack) == 0 {
		result = "<semantic format error, empty stack>"
	} else if len(v.stack) > 1 {
		var sb strings.Builder
		sb.WriteString("<semantic format error, big stack:")
		for _, s := range v.stack {
			sb.WriteString(" " + s)
		}
		sb.WriteString(">")
		result = sb.String()
	} else {
		result = v.stack[0]
	}
	fmt.Fprintf(fs, "%v", result)
}

type formattingVisitor struct {
	stack []string
}

func (v *formattingVisitor) push(s string) {
	v.stack = append(v.stack, s)
}

func (v *formattingVisitor) pop() string {
	l := len(v.stack)
	if l == 0 {
		return "<semantic format error, pop empty>"
	}
	s := v.stack[l-1]
	v.stack = v.stack[:l-1]
	return s
}

func (v *formattingVisitor) Visit(node Node) Visitor {
	switch n := node.(type) {
	case *LogicalExpression:
	case *BinaryExpression:
	case *MemberExpression:
	case *StringLiteral:
	case *FloatLiteral:
	case *IntegerLiteral:
	case *IdentifierExpression:
	default:
		// Do not recurse into unknown nodes, just push an error string
		// onto the stack
		v.push(fmt.Sprintf("<semantic format error, unknown node %T>", n))
		return nil
	}

	return v
}

func (v *formattingVisitor) Done(node Node) {
	switch n := node.(type) {
	case *LogicalExpression:
		rhs := v.pop()
		lhs := v.pop()
		v.push(lhs + " " + n.Operator.String() + " " + rhs)
	case *BinaryExpression:
		rhs := v.pop()
		lhs := v.pop()
		v.push(lhs + " " + n.Operator.String() + " " + rhs)
	case *MemberExpression:
		o := v.pop()
		v.push(o + "." + n.Property)
	case *StringLiteral:
		v.push("\"" + n.Value + "\"")
	case *FloatLiteral:
		// %f will add zeros even for whole numbers, it's possible to easily tell
		// the difference between integers and floats in output.
		v.push(fmt.Sprintf("%f", n.Value))
	case *IntegerLiteral:
		v.push(fmt.Sprintf("%v", n.Value))
	case *IdentifierExpression:
		v.push(n.Name)
	}
}
