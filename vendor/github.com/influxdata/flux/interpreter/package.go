package interpreter

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/internal/errors"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/values"
)

// Package is an importable package that can be used from another
// section of code. The package itself cannot have its attributes
// modified after creation, but the options may be changed.
type Package struct {
	// name is the name of the package.
	name string

	// path is the canonical import path that is used to import this package.
	path string

	// object contains the object properties of this package.
	object values.Object

	// sideEffects contains the side effects caused by this package.
	// This is currently unused.
	sideEffects []SideEffect
}

func NewPackageWithValues(name, path string, obj values.Object) *Package {
	return &Package{
		name:   name,
		path:   path,
		object: obj,
	}
}

func NewPackage(name string) *Package {
	obj := values.NewObject(semantic.NewObjectType(nil))
	return NewPackageWithValues(name, "", obj)
}

func (p *Package) Copy() *Package {
	var sideEffects []SideEffect
	if len(p.sideEffects) > 0 {
		sideEffects = make([]SideEffect, len(p.sideEffects))
		copy(sideEffects, p.sideEffects)
	}
	return &Package{
		name:        p.name,
		object:      p.object,
		sideEffects: sideEffects,
	}
}

// Name returns the package name.
func (p *Package) Name() string {
	return p.name
}

// Path returns the canonical import path for this package.
func (p *Package) Path() string {
	return p.path
}

func (p *Package) SideEffects() []SideEffect {
	return p.sideEffects
}
func (p *Package) Type() semantic.MonoType {
	return p.object.Type()
}
func (p *Package) Get(name string) (values.Value, bool) {
	return p.object.Get(name)
}
func (p *Package) Set(name string, v values.Value) {
	panic(errors.New(codes.Internal, "package members cannot be modified"))
}
func (p *Package) Len() int {
	return p.object.Len()
}
func (p *Package) Range(f func(name string, v values.Value)) {
	p.object.Range(f)
}
func (p *Package) IsNull() bool {
	return false
}
func (p *Package) Str() string {
	panic(values.UnexpectedKind(semantic.Object, semantic.String))
}
func (p *Package) Bytes() []byte {
	panic(values.UnexpectedKind(semantic.Object, semantic.Bytes))
}
func (p *Package) Int() int64 {
	panic(values.UnexpectedKind(semantic.Object, semantic.Int))
}
func (p *Package) UInt() uint64 {
	panic(values.UnexpectedKind(semantic.Object, semantic.UInt))
}
func (p *Package) Float() float64 {
	panic(values.UnexpectedKind(semantic.Object, semantic.Float))
}
func (p *Package) Bool() bool {
	panic(values.UnexpectedKind(semantic.Object, semantic.Bool))
}
func (p *Package) Time() values.Time {
	panic(values.UnexpectedKind(semantic.Object, semantic.Time))
}
func (p *Package) Duration() values.Duration {
	panic(values.UnexpectedKind(semantic.Object, semantic.Duration))
}
func (p *Package) Regexp() *regexp.Regexp {
	panic(values.UnexpectedKind(semantic.Object, semantic.Regexp))
}
func (p *Package) Array() values.Array {
	panic(values.UnexpectedKind(semantic.Object, semantic.Array))
}
func (p *Package) Object() values.Object {
	return p
}
func (p *Package) Function() values.Function {
	panic(values.UnexpectedKind(semantic.Object, semantic.Function))
}
func (p *Package) Dict() values.Dictionary {
	panic(values.UnexpectedKind(semantic.Object, semantic.Dictionary))
}
func (p *Package) Equal(rhs values.Value) bool {
	if p.Type() != rhs.Type() {
		return false
	}
	r := rhs.Object()
	if p.Len() != r.Len() {
		return false
	}
	equal := true
	p.Range(func(k string, v values.Value) {
		if !equal {
			return
		}
		val, ok := r.Get(k)
		equal = ok && v.Equal(val)
	})
	return equal
}

func (p *Package) String() string {
	var builder strings.Builder
	builder.WriteString("pkg{")
	i := 0
	p.Range(func(k string, v values.Value) {
		if _, ok := v.(*Package); ok {
			return
		}
		if i != 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(k)
		builder.WriteString(": ")
		builder.WriteString(fmt.Sprintf("%v", v.Type()))
		i++
	})
	builder.WriteRune('}')
	return builder.String()
}
