package builder

import (
	"fmt"
	"strings"
)

// merge
type MergeType struct {
	value Type
}

func (t MergeType) String() string {
	return t.value.String()
}

func (t MergeType) Name() string {
	return t.value.Name()
}

func Merge(value Type) MergeType {
	if _, ok := value.(HiddenType); ok {
		panic("HiddenType cannot be a child of MergeType, it must be the other way around.")
	}
	return MergeType{value}
}

// hidden field (::)
type HiddenType struct {
	value Type
}

func Hidden(value Type) HiddenType {
	if _, ok := value.(CommentType); ok {
		panic("CommentType cannot be a child of HiddenType, it must be the other way around.")
	}
	return HiddenType{value}
}

func (h HiddenType) Name() string {
	return h.value.Name()
}

func (h HiddenType) String() string {
	return h.value.String()
}

// arithmetic
type ArithType struct {
	named
	operator string
	operands []Type
}

func (m ArithType) String() string {
	rendered := make([]string, len(m.operands))
	for i, o := range m.operands {
		rendered[i] = o.String()
	}

	s := strings.Join(rendered, fmt.Sprintf(" %s ", m.operator))
	return s
}

func Add(name string, o ...Type) ArithType {
	return ArithType{named: named(name), operator: "+", operands: o}
}

func Sub(name string, o ...Type) ArithType {
	return ArithType{named: named(name), operator: "-", operands: o}
}

func Div(name string, o ...Type) ArithType {
	return ArithType{named: named(name), operator: "/", operands: o}
}

func Mul(name string, o ...Type) ArithType {
	return ArithType{named: named(name), operator: "*", operands: o}
}

func Mod(name string, o ...Type) ArithType {
	return ArithType{named: named(name), operator: "%", operands: o}
}

// string formatting
type SprintfType struct {
	named
	template string
	values   []Type
}

func Sprintf(name, format string, values ...Type) SprintfType {
	return SprintfType{
		named:    named(name),
		template: format,
		values:   values,
	}
}
