package values

import (
	"context"
	"fmt"
	"regexp"

	"github.com/influxdata/flux/semantic"
)

// Function represents a callable type
type Function interface {
	Value
	HasSideEffect() bool
	Call(ctx context.Context, args Object) (Value, error)
}

// NewFunction returns a new function value.
// This function will panic if it is passed anything other than a function type.
func NewFunction(name string, typ semantic.MonoType, call func(ctx context.Context, args Object) (Value, error), sideEffect bool) *function {
	if typ.Kind() != semantic.Fun {
		panic("expected function type, but instead got " + typ.String())
	}
	return &function{
		name:          name,
		t:             typ,
		call:          call,
		hasSideEffect: sideEffect,
	}
}

// function implements Value interface and more specifically the Function interface
type function struct {
	name          string
	t             semantic.MonoType
	call          func(ctx context.Context, args Object) (Value, error)
	hasSideEffect bool
}

func (f *function) IsNull() bool {
	return false
}
func (f *function) String() string {
	return fmt.Sprintf("%s()", f.name)
}

func (f *function) Type() semantic.MonoType {
	return f.t
}

func (f *function) Str() string {
	panic(UnexpectedKind(semantic.Function, semantic.String))
}

func (f *function) Bytes() []byte {
	panic(UnexpectedKind(semantic.Function, semantic.Bytes))
}

func (f *function) Int() int64 {
	panic(UnexpectedKind(semantic.Function, semantic.Int))
}

func (f *function) UInt() uint64 {
	panic(UnexpectedKind(semantic.Function, semantic.UInt))
}

func (f *function) Float() float64 {
	panic(UnexpectedKind(semantic.Function, semantic.Float))
}

func (f *function) Bool() bool {
	panic(UnexpectedKind(semantic.Function, semantic.Bool))
}

func (f *function) Time() Time {
	panic(UnexpectedKind(semantic.Function, semantic.Time))
}

func (f *function) Duration() Duration {
	panic(UnexpectedKind(semantic.Function, semantic.Duration))
}

func (f *function) Regexp() *regexp.Regexp {
	panic(UnexpectedKind(semantic.Function, semantic.Regexp))
}

func (f *function) Array() Array {
	panic(UnexpectedKind(semantic.Function, semantic.Function))
}

func (f *function) Object() Object {
	panic(UnexpectedKind(semantic.Function, semantic.Object))
}

func (f *function) Function() Function {
	return f
}

func (f *function) Dict() Dictionary {
	panic(UnexpectedKind(semantic.Function, semantic.Dictionary))
}

func (f *function) Equal(rhs Value) bool {
	if f.t != rhs.Type() {
		return false
	}
	v, ok := rhs.(*function)
	return ok && (f == v)
}

func (f *function) HasSideEffect() bool {
	return f.hasSideEffect
}

func (f *function) Call(ctx context.Context, args Object) (Value, error) {
	return f.call(ctx, args)
}
