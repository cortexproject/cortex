package values

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/internal/errors"
	"github.com/influxdata/flux/semantic"
)

// Array represents an sequence of elements
// All elements must be the same type
type Array interface {
	Value
	Get(i int) Value
	Set(i int, v Value)
	Append(v Value)
	Len() int
	Range(func(i int, v Value))
	Sort(func(i, j Value) bool)
}

type array struct {
	t        semantic.MonoType
	elements []Value
}

func NewArray(arrType semantic.MonoType) Array {
	return NewArrayWithBacking(arrType, nil)
}
func NewArrayWithBacking(arrType semantic.MonoType, elements []Value) Array {
	if arrType.Nature() != semantic.Array {
		panic(UnexpectedKind(arrType.Nature(), semantic.Array))
	}
	return &array{
		t:        arrType,
		elements: elements,
	}
}

func (a *array) IsNull() bool {
	return false
}
func (a *array) String() string {
	b := new(strings.Builder)
	b.WriteString("[")
	a.Range(func(i int, v Value) {
		if i != 0 {
			b.WriteString(", ")
		}
		fmt.Fprint(b, v)
	})
	b.WriteString("]")
	return b.String()
}

func (a *array) Type() semantic.MonoType {
	return a.t
}

func (a *array) Get(i int) Value {
	if i >= len(a.elements) {
		panic(errors.Newf(codes.Internal, "index out of bounds: i:%d len:%d", i, len(a.elements)))
	}
	return a.elements[i]
}

func (a *array) Set(i int, v Value) {
	if i >= len(a.elements) {
		panic(errors.Newf(codes.Internal, "index out of bounds: i:%d len:%d", i, len(a.elements)))
	}
	a.elements[i] = v
}

func (a *array) Append(v Value) {
	a.elements = append(a.elements, v)
}

func (a *array) Range(f func(i int, v Value)) {
	for i, v := range a.elements {
		f(i, v)
	}
}

func (a *array) Len() int {
	return len(a.elements)
}

func (a *array) Sort(f func(i, j Value) bool) {
	sort.Slice(a.elements, func(i, j int) bool {
		return f(a.elements[i], a.elements[j])
	})
}

func (a *array) Str() string {
	panic(UnexpectedKind(semantic.Array, semantic.String))
}
func (o *array) Bytes() []byte {
	panic(UnexpectedKind(semantic.Array, semantic.Bytes))
}
func (a *array) Int() int64 {
	panic(UnexpectedKind(semantic.Array, semantic.Int))
}
func (a *array) UInt() uint64 {
	panic(UnexpectedKind(semantic.Array, semantic.UInt))
}
func (a *array) Float() float64 {
	panic(UnexpectedKind(semantic.Array, semantic.Float))
}
func (a *array) Bool() bool {
	panic(UnexpectedKind(semantic.Array, semantic.Bool))
}
func (a *array) Time() Time {
	panic(UnexpectedKind(semantic.Array, semantic.Time))
}
func (a *array) Duration() Duration {
	panic(UnexpectedKind(semantic.Array, semantic.Duration))
}
func (a *array) Regexp() *regexp.Regexp {
	panic(UnexpectedKind(semantic.Array, semantic.Regexp))
}
func (a *array) Array() Array {
	return a
}
func (a *array) Object() Object {
	panic(UnexpectedKind(semantic.Array, semantic.Object))
}
func (a *array) Function() Function {
	panic(UnexpectedKind(semantic.Array, semantic.Function))
}
func (a *array) Dict() Dictionary {
	panic(UnexpectedKind(semantic.Array, semantic.Dictionary))
}
func (a *array) Equal(rhs Value) bool {
	if !a.Type().Equal(rhs.Type()) {
		return false
	}
	r := rhs.Array()
	if a.Len() != r.Len() {
		return false
	}
	length := a.Len()
	for i := 0; i < length; i++ {
		aVal := a.Get(i)
		rVal := r.Get(i)
		if !aVal.Equal(rVal) {
			return false
		}
	}
	return true
}
