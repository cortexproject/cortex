package builder

import (
	"fmt"
	"strings"
)

// function call
type CallType struct {
	named
	funcName string
	args     []Type
}

func (c CallType) String() string {
	args := argsString(c.args, len(c.args) > 3)
	return fmt.Sprintf("%s(%s)", c.funcName, args)
}

func Call(name, funcName string, args []Type) CallType {
	for k, v := range args {
		if v == nil {
			panic(fmt.Sprintf("argument `%v` in call to `%s` is nil", k, funcName))
		}
	}

	return CallType{
		named:    named(name),
		funcName: funcName,
		args:     args,
	}
}

// CallChain allows to chain multiple calls
func CallChain(name string, calls ...CallType) CallType {
	if len(calls) == 1 {
		panic("callChain with a single call is redundant")
	}

	ln := ""
	if len(calls) > 1 {
		ln = "\n"
	}

	var last Type = Ref("", "")
	for i, c := range calls {
		last = Call("",
			strings.TrimPrefix(
				fmt.Sprintf("%s%s.%s", last.String(), ln, c.funcName),
				ln+".",
			),
			c.args,
		)

		if i == len(calls)-1 {
			l := last.(CallType)
			l.named = named(name)
			return l
		}
	}

	panic("loop did not return. This should never happen")
}
