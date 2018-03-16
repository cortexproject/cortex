package builder_test

import (
	"github.com/lann/builder"
	"reflect"
	"testing"
)

type Foo struct {
	X   int
	Y   int
	I   interface{}
	Add []int
}

type fooBuilder builder.Builder

func (b fooBuilder) X(i int) fooBuilder {
	return builder.Set(b, "X", i).(fooBuilder)
}

func (b fooBuilder) Y(i int) fooBuilder {
	return builder.Set(b, "Y", i).(fooBuilder)
}

func (b fooBuilder) I(i interface{}) fooBuilder {
	return builder.Set(b, "I", i).(fooBuilder)
}

func (b fooBuilder) Add(i int) fooBuilder {
	return builder.Append(b, "Add", i).(fooBuilder)
}

var FooBuilder = builder.Register(fooBuilder{}, Foo{}).(fooBuilder)

type unregBuilder builder.Builder

func (b unregBuilder) Add(i int) unregBuilder {
	return builder.Append(b, "X", i).(unregBuilder)
}

func assertInt(t *testing.T, b fooBuilder, key string, val int) {
	v, ok := builder.Get(b, key)
	if !ok {
		t.Errorf("key %v not set", key)
		return
	}
	i := v.(int)
	if i != val {
		t.Errorf("expected %d, got %d", val, i)
	}
	return
}

func TestBuilder(t *testing.T) {
	b := FooBuilder.X(1).Y(2)
	assertInt(t, b, "X", 1)
	assertInt(t, b, "Y", 2)
	v, _ := builder.Get(b, "Z")
	if v != nil {
		t.Errorf("expected nil, got %v", v)
	}
}

func TestDelete(t *testing.T) {
	b := builder.Delete(FooBuilder.X(1), "X")
	_, ok := builder.Get(b, "X")
	if ok {
		t.Fatalf("key %v not deleted", "X")
	}
}

func TestAppend(t *testing.T) {
	b := FooBuilder.Add(1).Add(2)
	v, ok := builder.Get(b, "Add")
	if !ok {
		t.Fatalf("key %v not set", "Add")
	}
	a := v.([]int)
	if len(a) != 2 {
		t.Fatalf("wrong len %d", len(a))
	}
	i := a[0]
	j := a[1]
	if i != 1 || j != 2 {
		t.Errorf("expected [1, 2], got [%d, %d]", i, j)
	}
}

func TestExtendNil(t *testing.T) {
	b := builder.Extend(FooBuilder, "Add", nil)
	_, ok := builder.Get(b, "X")
	if ok {
		t.Fatalf("key %v set unexpectedly", "Add")
	}
}

func TestExtendPanic(t *testing.T) {
	var panicVal *reflect.ValueError
	func() {
		defer func() { panicVal = recover().(*reflect.ValueError) }()
		builder.Extend(FooBuilder, "Add", Foo{})
	}()
	if panicVal == nil {
		t.Errorf("expected panic, didn't")
	}
}

func TestSplitChain(t *testing.T) {
	b1 := FooBuilder.X(1)
	b2 := b1.X(2)
	b3 := b1.X(3)
	assertInt(t, b1, "X", 1)
	assertInt(t, b2, "X", 2)
	assertInt(t, b3, "X", 3)
}

func TestGetMap(t *testing.T) {
	b := FooBuilder.X(1).Y(2).Add(3).Add(4)
	m := builder.GetMap(b)
	expected := map[string]interface{}{
		"X":   1,
		"Y":   2,
		"Add": []int{3, 4},
	}
	if !reflect.DeepEqual(m, expected) {
		t.Errorf("expected %v, got %v", expected, m)
	}
}

func TestGetStruct(t *testing.T) {
	b := FooBuilder.X(1).Y(2).Add(3).Add(4)
	s := builder.GetStruct(b).(Foo)
	expected := Foo{X: 1, Y: 2, Add: []int{3, 4}}
	if !reflect.DeepEqual(s, expected) {
		t.Errorf("expected %v, got %v", expected, s)
	}
}

func TestGetStructLike(t *testing.T) {
	b := FooBuilder.X(1).Y(2).Add(3).Add(4)
	s := builder.GetStructLike(b, Foo{}).(Foo)
	expected := Foo{X: 1, Y: 2, Add: []int{3, 4}}
	if !reflect.DeepEqual(s, expected) {
		t.Errorf("expected %v, got %v", expected, s)
	}
}

func TestZeroBuilder(t *testing.T) {
	f := builder.GetStruct(fooBuilder{}.X(1)).(Foo)
	if f.X != 1 {
		t.Error("nil builder failed")
	}
}

func TestUnregisteredBuilder(t *testing.T) {
	b := unregBuilder{}.Add(1)

	x, _ := builder.Get(b, "X")
	expected := []interface{}{1}
	if !reflect.DeepEqual(x.([]interface{}), expected) {
		t.Errorf("expected %v, got %v", expected, x)
	}

	x = builder.GetMap(b)["X"]
	expected = []interface{}{1}
	if !reflect.DeepEqual(x.([]interface{}), expected) {
		t.Errorf("expected %v, got %v", expected, x)
	}

	s := builder.GetStruct(b)
	if s != nil {
		t.Errorf("expected nil, got %v", s)
	}
}

func TestSetNil(t *testing.T) {
	b := FooBuilder.I(nil)
	builder.GetStruct(b)
}

func TestSetInvalidNil(t *testing.T) {
	var panicVal interface{}
	func() {
		defer func() { panicVal = recover() }()
		b := builder.Set(FooBuilder, "X", nil)
		builder.GetStruct(b)
	}()
	if panicVal == nil {
		t.Errorf("expected panic, didn't")
	}
}
