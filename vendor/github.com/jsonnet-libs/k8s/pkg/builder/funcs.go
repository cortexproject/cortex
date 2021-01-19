package builder

import (
	"fmt"
	"strings"
)

// function definition
type FuncType struct {
	named
	args  []Type
	value Type
	large bool
}

func Func(name string, args []Type, returns Type) FuncType {
	return FuncType{
		named: named(name),
		args:  args,
		value: returns,
	}
}

func LargeFunc(name string, args []Type, returns Type) FuncType {
	f := Func(name, args, returns)
	f.large = true
	return f
}

func (f FuncType) Args() string {
	s := argsString(f.args, f.large)
	if f.large {
		return s + "\n"
	}
	return s
}

func argsString(m []Type, breakLine bool) string {
	sep := SeparatorConcise
	if breakLine {
		sep = SeparatorLong + "  "
	}

	s := ""
	if breakLine {
		s = sep
	}

	for _, v := range m {
		if _, ok := v.(RequiredArgType); ok {
			s += fmt.Sprintf("%s"+sep, v.Name())
		} else {
			s += fmt.Sprintf("%s=%s"+sep, v.Name(), v.String())
		}
	}

	if breakLine {
		s = strings.TrimPrefix(s, ",")
		s = strings.TrimSuffix(s, sep)
		return s
	}

	s = strings.TrimSuffix(s, sep)
	return s
}

func (f FuncType) String() string {
	return f.value.String()
}

// function arguments
func Args(args ...Type) []Type {
	return args
}

// required arguments (no default value)
type RequiredArgType struct {
	value Type
}

func (r RequiredArgType) Name() string {
	return r.value.Name()
}

func (r RequiredArgType) String() string {
	return r.value.String()
}

func Required(t Type) RequiredArgType {
	return RequiredArgType{t}
}
