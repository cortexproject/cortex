package values

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/internal/errors"
	"github.com/influxdata/flux/semantic"
)

type Object interface {
	Value
	Get(name string) (Value, bool)

	// Set will set the object value for the given key.
	// The key must be part of the object property's type and the
	// value that is set must match that type. It is undefined
	// behavior to set a non-existant value or to set a value
	// with the wrong type.
	Set(name string, v Value)

	Len() int
	Range(func(name string, v Value))
}

// labelSet is a set of string labels.
type labelSet []string

// allLabels is a sentinal values indicating the set is the infinite set of all possible string labels.
const allLabels = "-all-"

// AllLabels returns a label set that represents the infinite set of all possible string labels.
func AllLabels() labelSet {
	return labelSet{allLabels}
}

func (s labelSet) String() string {
	if s.isAllLabels() {
		return "L"
	}
	if len(s) == 0 {
		return "∅"
	}
	var builder strings.Builder
	builder.WriteString("(")
	for i, l := range s {
		if i != 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(l)
	}
	builder.WriteString(")")
	return builder.String()
}

func (s labelSet) isAllLabels() bool {
	return len(s) == 1 && s[0] == allLabels
}

type object struct {
	labels labelSet
	values []Value
	typ    semantic.MonoType
}

// NewObject will create a new object with the given type.
// The type must be of kind Row.
// The new object will be uninitialized and must be constructed
// with Set on each of the property keys before it is used.
func NewObject(typ semantic.MonoType) Object {
	n, err := typ.NumProperties()
	if err != nil {
		panic(err)
	}
	labels := make(labelSet, n)
	for i := 0; i < len(labels); i++ {
		rp, err := typ.RecordProperty(i)
		if err != nil {
			panic(err)
		}
		labels[i] = rp.Name()
	}
	return &object{
		labels: labels,
		values: make([]Value, n),
		typ:    typ,
	}
}

// ObjectSetter will set the value for the key.
// If the key already exists, it will be overwritten with the new value.
type ObjectSetter func(k string, v Value)

// BuildObject will build an object by setting key/value pairs.
// The records in the object get constructed in the order they
// are set and resetting a key will overwrite a previous value,
// but will retain the existing position.
func BuildObject(fn func(set ObjectSetter) error) (Object, error) {
	return BuildObjectWithSize(0, fn)
}

// BuildObjectWithSize will build an object with an initial size.
func BuildObjectWithSize(sz int, fn func(set ObjectSetter) error) (Object, error) {
	var (
		keys   []string
		values []Value
	)
	if sz > 0 {
		keys = make([]string, 0, sz)
		values = make([]Value, 0, sz)
	}
	if err := fn(func(k string, v Value) {
		for i := 0; i < len(keys); i++ {
			if keys[i] == k {
				values[i] = v
				return
			}
		}
		keys = append(keys, k)
		values = append(values, v)
	}); err != nil {
		return nil, err
	}

	properties := make([]semantic.PropertyType, len(keys))
	for i, k := range keys {
		properties[i] = semantic.PropertyType{
			Key:   []byte(k),
			Value: values[i].Type(),
		}
	}
	typ := semantic.NewObjectType(properties)

	object := NewObject(typ)
	for i, k := range keys {
		object.Set(k, values[i])
	}
	return object, nil
}

func NewObjectWithValues(vals map[string]Value) Object {
	properties := make([]semantic.PropertyType, 0, len(vals))
	for key, value := range vals {
		properties = append(properties, semantic.PropertyType{
			Key:   []byte(key),
			Value: value.Type(),
		})
	}

	object := NewObject(semantic.NewObjectType(properties))
	object.Range(func(name string, _ Value) {
		object.Set(name, vals[name])
	})
	return object
}

func (o *object) IsNull() bool {
	return false
}
func (o *object) String() string {
	b := new(strings.Builder)
	b.WriteString("{")
	i := 0
	o.Range(func(k string, v Value) {
		if i != 0 {
			b.WriteString(", ")
		}
		i++
		b.WriteString(k)
		b.WriteString(": ")
		fmt.Fprint(b, v)
	})
	b.WriteString("}")
	return b.String()
}

func (o *object) Type() semantic.MonoType {
	return o.typ
}

func (o *object) Set(k string, v Value) {
	// Find the index of the object.
	for i, l := range o.labels {
		if k == l {
			o.values[i] = v
			return
		}
	}
	panic(errors.Newf(codes.Internal, "key %q not defined in object", k))
}

func (o *object) Get(name string) (Value, bool) {
	for i, l := range o.labels {
		if name == l {
			return o.values[i], true
		}
	}
	return Null, false
}
func (o *object) Len() int {
	return len(o.values)
}

func (o *object) Range(f func(name string, v Value)) {
	for i, l := range o.labels {
		f(l, o.values[i])
	}
}

func (o *object) Str() string {
	panic(UnexpectedKind(semantic.Object, semantic.String))
}
func (o *object) Bytes() []byte {
	panic(UnexpectedKind(semantic.Object, semantic.Bytes))
}
func (o *object) Int() int64 {
	panic(UnexpectedKind(semantic.Object, semantic.Int))
}
func (o *object) UInt() uint64 {
	panic(UnexpectedKind(semantic.Object, semantic.UInt))
}
func (o *object) Float() float64 {
	panic(UnexpectedKind(semantic.Object, semantic.Float))
}
func (o *object) Bool() bool {
	panic(UnexpectedKind(semantic.Object, semantic.Bool))
}
func (o *object) Time() Time {
	panic(UnexpectedKind(semantic.Object, semantic.Time))
}
func (o *object) Duration() Duration {
	panic(UnexpectedKind(semantic.Object, semantic.Duration))
}
func (o *object) Regexp() *regexp.Regexp {
	panic(UnexpectedKind(semantic.Object, semantic.Regexp))
}
func (o *object) Array() Array {
	panic(UnexpectedKind(semantic.Object, semantic.Array))
}
func (o *object) Object() Object {
	return o
}
func (o *object) Function() Function {
	panic(UnexpectedKind(semantic.Object, semantic.Function))
}
func (o *object) Dict() Dictionary {
	panic(UnexpectedKind(semantic.Object, semantic.Dictionary))
}
func (o *object) Equal(rhs Value) bool {
	if rhs.Type().Nature() != semantic.Object {
		return false
	}
	r := rhs.Object()
	if o.Len() != r.Len() {
		return false
	}
	for i, l := range o.labels {
		val, ok := r.Get(l)
		if !ok || !o.values[i].Equal(val) {
			return false
		}
	}
	return true
}
