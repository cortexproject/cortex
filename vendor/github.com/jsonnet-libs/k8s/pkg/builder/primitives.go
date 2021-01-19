package builder

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// strings
type StringType struct {
	named
	value string
}

func String(name, value string) StringType {
	return StringType{named: named(name), value: value}
}

func (s StringType) String() string {
	data, err := json.Marshal(s.value)
	if err != nil {
		panic(err)
	}
	return string(data)
}

// floats
type FloatType struct {
	named
	value float64
}

func Float(name string, value float64) FloatType {
	return FloatType{named: named(name), value: value}
}

func (f FloatType) String() string {
	return fmt.Sprintf("%f", f.value)
}

// ints
type IntType struct {
	named
	value int
}

func Int(name string, value int) IntType {
	return IntType{named: named(name), value: value}
}

func (s IntType) String() string {
	return strconv.Itoa(s.value)
}

// bools
type BoolType struct {
	named
	value bool
}

func Bool(name string, value bool) BoolType {
	return BoolType{named: named(name), value: value}
}

func (s BoolType) String() string {
	return fmt.Sprintf(`%v`, s.value)
}
