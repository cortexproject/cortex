// Package values declares the flux data types and implements them.
package values

import (
	"bytes"
	"fmt"
	"regexp"
	"runtime/debug"

	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/internal/errors"
	"github.com/influxdata/flux/semantic"
)

type Typer interface {
	Type() semantic.MonoType
}

type Value interface {
	Typer
	IsNull() bool
	Str() string
	Bytes() []byte
	Int() int64
	UInt() uint64
	Float() float64
	Bool() bool
	Time() Time
	Duration() Duration
	Regexp() *regexp.Regexp
	Array() Array
	Object() Object
	Function() Function
	Dict() Dictionary
	Equal(Value) bool
}

type ValueStringer interface {
	String() string
}

type value struct {
	t semantic.MonoType
	v interface{}
}

func (v value) Type() semantic.MonoType {
	return v.t
}
func (v value) IsNull() bool {
	return v.v == nil
}
func (v value) Str() string {
	CheckKind(v.t.Nature(), semantic.String)
	return v.v.(string)
}
func (v value) Bytes() []byte {
	CheckKind(v.t.Nature(), semantic.Bytes)
	return v.v.([]byte)
}
func (v value) Int() int64 {
	CheckKind(v.t.Nature(), semantic.Int)
	return v.v.(int64)
}
func (v value) UInt() uint64 {
	CheckKind(v.t.Nature(), semantic.UInt)
	return v.v.(uint64)
}
func (v value) Float() float64 {
	CheckKind(v.t.Nature(), semantic.Float)
	return v.v.(float64)
}
func (v value) Bool() bool {
	CheckKind(v.t.Nature(), semantic.Bool)
	return v.v.(bool)
}
func (v value) Time() Time {
	CheckKind(v.t.Nature(), semantic.Time)
	return v.v.(Time)
}
func (v value) Duration() Duration {
	CheckKind(v.t.Nature(), semantic.Duration)
	return v.v.(Duration)
}
func (v value) Regexp() *regexp.Regexp {
	CheckKind(v.t.Nature(), semantic.Regexp)
	return v.v.(*regexp.Regexp)
}
func (v value) Array() Array {
	CheckKind(v.t.Nature(), semantic.Array)
	return v.v.(Array)
}
func (v value) Object() Object {
	CheckKind(v.t.Nature(), semantic.Object)
	return v.v.(Object)
}
func (v value) Function() Function {
	CheckKind(v.t.Nature(), semantic.Function)
	return v.v.(Function)
}
func (v value) Dict() Dictionary {
	CheckKind(v.t.Nature(), semantic.Dictionary)
	return v.v.(Dictionary)
}
func (v value) Equal(r Value) bool {
	if v.Type().Nature() != r.Type().Nature() {
		return false
	}

	if v.IsNull() || r.IsNull() {
		return false
	}

	switch k := v.Type().Nature(); k {
	case semantic.Bool:
		return v.Bool() == r.Bool()
	case semantic.UInt:
		return v.UInt() == r.UInt()
	case semantic.Int:
		return v.Int() == r.Int()
	case semantic.Float:
		return v.Float() == r.Float()
	case semantic.String:
		return v.Str() == r.Str()
	case semantic.Bytes:
		return bytes.Equal(v.Bytes(), r.Bytes())
	case semantic.Time:
		return v.Time() == r.Time()
	case semantic.Duration:
		return v.Duration() == r.Duration()
	case semantic.Regexp:
		return v.Regexp().String() == r.Regexp().String()
	case semantic.Object:
		return v.Object().Equal(r.Object())
	case semantic.Array:
		return v.Array().Equal(r.Array())
	case semantic.Function:
		return v.Function().Equal(r.Function())
	case semantic.Dictionary:
		return v.Dict().Equal(r.Dict())
	default:
		return false
	}
}

func (v value) String() string {
	return fmt.Sprintf("%v", v.v)
}

var (
	// InvalidValue is a non nil value who's type is semantic.Invalid
	InvalidValue = value{}

	// Null is an untyped nil value.
	Null = null{}
)

// Unwrap will extract the primitive value from the Value interface.
func Unwrap(v Value) interface{} {
	if v.IsNull() {
		return nil
	}
	switch n := v.Type().Nature(); n {
	case semantic.String:
		return v.Str()
	case semantic.Bytes:
		return v.Bytes()
	case semantic.Int:
		return v.Int()
	case semantic.UInt:
		return v.UInt()
	case semantic.Float:
		return v.Float()
	case semantic.Bool:
		return v.Bool()
	case semantic.Time:
		return v.Time()
	case semantic.Duration:
		return v.Duration()
	case semantic.Regexp:
		return v.Regexp()
	case semantic.Array:
		arr := v.Array()
		a := make([]interface{}, arr.Len())
		arr.Range(func(i int, v Value) {
			val := Unwrap(v)
			a[i] = val
		})
		return a
	case semantic.Object:
		obj := v.Object()
		o := make(map[string]interface{}, obj.Len())
		obj.Range(func(k string, v Value) {
			val := Unwrap(v)
			o[k] = val
		})
		return o
	case semantic.Dictionary:
		dict := v.Dict()
		d := make(map[interface{}]interface{}, dict.Len())
		dict.Range(func(key, value Value) {
			k := Unwrap(key)
			d[k] = Unwrap(value)
		})
		return d
	case semantic.Function:
		// there is no primitive value for a Function object, just return itself.
		return v
	default:
		panic(errors.Newf(codes.Unknown, "cannot unwrap a %v type value", n))
	}
}

// New constructs a new Value by inferring the type from the interface.
// Note this method will panic if passed a nil value. If the interface
// does not translate to a valid Value type, then InvalidValue is returned.
func New(v interface{}) Value {
	if v == nil {
		return Null
	}
	switch v := v.(type) {
	case string:
		return NewString(v)
	case []byte:
		return NewBytes(v)
	case int64:
		return NewInt(v)
	case uint64:
		return NewUInt(v)
	case float64:
		return NewFloat(v)
	case bool:
		return NewBool(v)
	case Time:
		return NewTime(v)
	case Duration:
		return NewDuration(v)
	case *regexp.Regexp:
		return NewRegexp(v)
	default:
		return InvalidValue
	}
}

func NewNull(t semantic.MonoType) Value {
	return value{
		t: t,
		v: nil,
	}
}

func NewString(v string) Value {
	return value{
		t: semantic.BasicString,
		v: v,
	}
}
func NewBytes(v []byte) Value {
	return value{
		t: semantic.BasicBytes,
		v: v,
	}
}
func NewInt(v int64) Value {
	return value{
		t: semantic.BasicInt,
		v: v,
	}
}
func NewUInt(v uint64) Value {
	return value{
		t: semantic.BasicUint,
		v: v,
	}
}
func NewFloat(v float64) Value {
	return value{
		t: semantic.BasicFloat,
		v: v,
	}
}
func NewTime(v Time) Value {
	return value{
		t: semantic.BasicTime,
		v: v,
	}
}
func NewDuration(v Duration) Value {
	return value{
		t: semantic.BasicDuration,
		v: v,
	}
}
func NewRegexp(v *regexp.Regexp) Value {
	return value{
		t: semantic.BasicRegexp,
		v: v,
	}
}

func UnexpectedKind(got, exp semantic.Nature) error {
	return errors.Newf(codes.Internal, "unexpected kind: got %q expected %q, trace: %s", got, exp, string(debug.Stack()))
}

// CheckKind panics if got != exp.
func CheckKind(got, exp semantic.Nature) {
	if got != exp {
		panic(UnexpectedKind(got, exp))
	}
}

// IsTimeable checks if value v is Timeable.
func IsTimeable(v Value) bool {
	return v.Type().Nature() == semantic.Time || v.Type().Nature() == semantic.Duration
}

type null struct{}

func (n null) Type() semantic.MonoType { return semantic.MonoType{} }
func (n null) IsNull() bool            { return true }
func (n null) Str() string             { panic(UnexpectedKind(semantic.Invalid, semantic.String)) }
func (n null) Bytes() []byte           { panic(UnexpectedKind(semantic.Invalid, semantic.Bytes)) }
func (n null) Int() int64              { panic(UnexpectedKind(semantic.Invalid, semantic.Int)) }
func (n null) UInt() uint64            { panic(UnexpectedKind(semantic.Invalid, semantic.UInt)) }
func (n null) Float() float64          { panic(UnexpectedKind(semantic.Invalid, semantic.Float)) }
func (n null) Bool() bool              { panic(UnexpectedKind(semantic.Invalid, semantic.Bool)) }
func (n null) Time() Time              { panic(UnexpectedKind(semantic.Invalid, semantic.Time)) }
func (n null) Duration() Duration      { panic(UnexpectedKind(semantic.Invalid, semantic.Duration)) }
func (n null) Regexp() *regexp.Regexp  { panic(UnexpectedKind(semantic.Invalid, semantic.Regexp)) }
func (n null) Array() Array            { panic(UnexpectedKind(semantic.Invalid, semantic.Array)) }
func (n null) Object() Object          { panic(UnexpectedKind(semantic.Invalid, semantic.Object)) }
func (n null) Function() Function      { panic(UnexpectedKind(semantic.Invalid, semantic.Function)) }
func (n null) Dict() Dictionary        { panic(UnexpectedKind(semantic.Invalid, semantic.Dictionary)) }
func (n null) Equal(Value) bool        { return false }
