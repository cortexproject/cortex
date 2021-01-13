package values

import (
	"regexp"

	"github.com/benbjohnson/immutable"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/internal/errors"
	"github.com/influxdata/flux/semantic"
)

// Dictionary defines the interface for a dictionary.
//
// A Dictionary is immutable. Changes to a Dictionary
// will return a new Dictionary.
type Dictionary interface {
	Value

	// Get will retrieve a Value out of the Dictionary.
	// If the key is not present, the def Value will
	// be returned instead.
	Get(key, def Value) Value

	// Insert will insert a Value into the Dictionary
	// using the key and value. It will return a new
	// Dictionary with the key/value inserted. If the
	// key was already in the Dictionary, it will
	// be replaced.
	//
	// Attempting to insert a null value for the key
	// will return an error.
	Insert(key, value Value) (Dictionary, error)

	// Remove will remove the key/value pair that
	// matches with the key. It will return a new
	// Dictionary with the key/value removed.
	Remove(key Value) Dictionary

	// Range will iterate over each element in
	// the Dictionary.
	Range(func(key, value Value))

	// Len returns the number of elements inside of
	// Dictionary.
	Len() int
}

// NewEmptyDict will construct an empty dictionary
func NewEmptyDict(ty semantic.MonoType) Dictionary {
	return emptyDict{t: ty}
}

type emptyDict struct {
	t semantic.MonoType
}

func (d emptyDict) Get(key, def Value) Value {
	return def
}

func (d emptyDict) Insert(key, val Value) (Dictionary, error) {
	builder := NewDictBuilder(semantic.NewDictType(key.Type(), val.Type()))
	if err := builder.Insert(key, val); err != nil {
		return nil, err
	}
	return builder.Dict(), nil
}

func (d emptyDict) Remove(key Value) Dictionary {
	return d
}

func (d emptyDict) Range(f func(key, value Value)) {
}

func (d emptyDict) Len() int {
	return 0
}

func (d emptyDict) Type() semantic.MonoType { return d.t }
func (d emptyDict) IsNull() bool            { return false }
func (d emptyDict) Str() string {
	panic(UnexpectedKind(semantic.Dictionary, semantic.String))
}
func (d emptyDict) Bytes() []byte {
	panic(UnexpectedKind(semantic.Dictionary, semantic.Bytes))
}
func (d emptyDict) Int() int64 {
	panic(UnexpectedKind(semantic.Dictionary, semantic.Int))
}
func (d emptyDict) UInt() uint64 {
	panic(UnexpectedKind(semantic.Dictionary, semantic.UInt))
}
func (d emptyDict) Float() float64 {
	panic(UnexpectedKind(semantic.Dictionary, semantic.Float))
}
func (d emptyDict) Bool() bool {
	panic(UnexpectedKind(semantic.Dictionary, semantic.Bool))
}
func (d emptyDict) Time() Time {
	panic(UnexpectedKind(semantic.Dictionary, semantic.Time))
}
func (d emptyDict) Duration() Duration {
	panic(UnexpectedKind(semantic.Dictionary, semantic.Duration))
}
func (d emptyDict) Regexp() *regexp.Regexp {
	panic(UnexpectedKind(semantic.Dictionary, semantic.Regexp))
}
func (d emptyDict) Array() Array {
	panic(UnexpectedKind(semantic.Dictionary, semantic.Array))
}
func (d emptyDict) Object() Object {
	panic(UnexpectedKind(semantic.Dictionary, semantic.Object))
}
func (d emptyDict) Function() Function {
	panic(UnexpectedKind(semantic.Dictionary, semantic.Function))
}
func (d emptyDict) Dict() Dictionary {
	return d
}

func (d emptyDict) Equal(v Value) bool {
	return d.t.Equal(v.Type()) && v.Dict().Len() == 0
}

type dict struct {
	t    semantic.MonoType
	data *immutable.SortedMap
}

func (d dict) Get(key, def Value) Value {
	if !key.IsNull() {
		v, ok := d.data.Get(key)
		if ok {
			return v.(Value)
		}
	}
	return def
}

func (d dict) Insert(key, value Value) (Dictionary, error) {
	if key.IsNull() {
		return nil, errors.New(codes.Invalid, "null value cannot be used as a dictionary key")
	}
	data := d.data.Set(key, value)
	return dict{t: d.t, data: data}, nil
}

func (d dict) Remove(key Value) Dictionary {
	if key.IsNull() {
		return d
	}
	data := d.data.Delete(key)
	return dict{t: d.t, data: data}
}

func (d dict) Range(f func(key, value Value)) {
	itr := d.data.Iterator()
	for {
		key, value := itr.Next()
		if key == nil {
			return
		}
		f(key.(Value), value.(Value))
	}
}

func (d dict) Len() int {
	return d.data.Len()
}

func (d dict) Type() semantic.MonoType { return d.t }
func (d dict) IsNull() bool            { return false }
func (d dict) Str() string {
	panic(UnexpectedKind(semantic.Dictionary, semantic.String))
}
func (d dict) Bytes() []byte {
	panic(UnexpectedKind(semantic.Dictionary, semantic.Bytes))
}
func (d dict) Int() int64 {
	panic(UnexpectedKind(semantic.Dictionary, semantic.Int))
}
func (d dict) UInt() uint64 {
	panic(UnexpectedKind(semantic.Dictionary, semantic.UInt))
}
func (d dict) Float() float64 {
	panic(UnexpectedKind(semantic.Dictionary, semantic.Float))
}
func (d dict) Bool() bool {
	panic(UnexpectedKind(semantic.Dictionary, semantic.Bool))
}
func (d dict) Time() Time {
	panic(UnexpectedKind(semantic.Dictionary, semantic.Time))
}
func (d dict) Duration() Duration {
	panic(UnexpectedKind(semantic.Dictionary, semantic.Duration))
}
func (d dict) Regexp() *regexp.Regexp {
	panic(UnexpectedKind(semantic.Dictionary, semantic.Regexp))
}
func (d dict) Array() Array {
	panic(UnexpectedKind(semantic.Dictionary, semantic.Array))
}
func (d dict) Object() Object {
	panic(UnexpectedKind(semantic.Dictionary, semantic.Object))
}
func (d dict) Function() Function {
	panic(UnexpectedKind(semantic.Dictionary, semantic.Function))
}
func (d dict) Dict() Dictionary {
	return d
}

func (d dict) Equal(v Value) bool {
	if !d.t.Equal(v.Type()) {
		return false
	}

	other := v.Dict()
	if d.data.Len() != other.Len() {
		return false
	}

	equal := true
	other.Range(func(key, value Value) {
		if !equal {
			return
		}

		v, ok := d.data.Get(key)
		if !ok {
			equal = false
			return
		}
		equal = value.Equal(v.(Value))
	})
	return equal
}

type (
	intComparer    struct{}
	uintComparer   struct{}
	floatComparer  struct{}
	stringComparer struct{}
	timeComparer   struct{}
)

func (c intComparer) Compare(a, b interface{}) int {
	if i, j := a.(Value).Int(), b.(Value).Int(); i < j {
		return -1
	} else if i > j {
		return 1
	}
	return 0
}

func (c uintComparer) Compare(a, b interface{}) int {
	if i, j := a.(Value).UInt(), b.(Value).UInt(); i < j {
		return -1
	} else if i > j {
		return 1
	}
	return 0
}

func (c floatComparer) Compare(a, b interface{}) int {
	if i, j := a.(Value).Float(), b.(Value).Float(); i < j {
		return -1
	} else if i > j {
		return 1
	}
	return 0
}

func (c stringComparer) Compare(a, b interface{}) int {
	if i, j := a.(Value).Str(), b.(Value).Str(); i < j {
		return -1
	} else if i > j {
		return 1
	}
	return 0
}

func (c timeComparer) Compare(a, b interface{}) int {
	if i, j := a.(Value).Time(), b.(Value).Time(); i < j {
		return -1
	} else if i > j {
		return 1
	}
	return 0
}

func dictComparer(dictType semantic.MonoType) immutable.Comparer {
	if dictType.Nature() != semantic.Dictionary {
		panic(UnexpectedKind(dictType.Nature(), semantic.Dictionary))
	}
	keyType, err := dictType.KeyType()
	if err != nil {
		panic(err)
	}
	switch n := keyType.Nature(); n {
	case semantic.Int:
		return intComparer{}
	case semantic.UInt:
		return uintComparer{}
	case semantic.Float:
		return floatComparer{}
	case semantic.String:
		return stringComparer{}
	case semantic.Time:
		return timeComparer{}
	default:
		panic(errors.Newf(codes.Internal, "invalid key nature: %s", n))
	}
}

// NewDict will construct a new Dictionary with the given key type.
func NewDict(dictType semantic.MonoType) Dictionary {
	return dict{
		t: dictType,
		data: immutable.NewSortedMap(
			dictComparer(dictType),
		),
	}
}

// DictionaryBuilder can be used to construct a Dictionary
// with in-place memory instead of successive Insert calls
// that create new Dictionary values.
type DictionaryBuilder struct {
	t semantic.MonoType
	b *immutable.SortedMapBuilder
}

// NewDictBuilder will create a new DictionaryBuilder for the given
// key type.
func NewDictBuilder(dictType semantic.MonoType) DictionaryBuilder {
	builder := immutable.NewSortedMapBuilder(
		immutable.NewSortedMap(
			dictComparer(dictType),
		),
	)
	return DictionaryBuilder{t: dictType, b: builder}
}

// Dict will construct a new Dictionary using the inserted values.
func (d *DictionaryBuilder) Dict() Dictionary {
	return dict{
		t:    d.t,
		data: d.b.Map(),
	}
}

// Get will retrieve a Value if it is present.
func (d *DictionaryBuilder) Get(key Value) (Value, bool) {
	v, ok := d.b.Get(key)
	if !ok {
		return nil, false
	}
	return v.(Value), true
}

// Insert will insert a new key/value pair into the Dictionary.
func (d *DictionaryBuilder) Insert(key, value Value) error {
	if key.IsNull() {
		return errors.New(codes.Invalid, "null value cannot be used as a dictionary key")
	}
	d.b.Set(key, value)
	return nil
}

// Remove will remove a key/value pair from the Dictionary.
func (d *DictionaryBuilder) Remove(key Value) {
	if !key.IsNull() {
		d.b.Delete(key)
	}
}
