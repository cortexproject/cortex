// Package complete provides types to aid with auto-completion of Flux scripts in editors.
package complete

import (
	"errors"
	"fmt"
	"sort"

	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/values"
)

// FunctionSuggestion provides suggestion information about a function.
type FunctionSuggestion struct {
	Params map[string]string
}

// Completer provides methods for suggestions in Flux queries.
type Completer struct {
	scope values.Scope
}

// NewCompleter creates a new completer from scope.
func NewCompleter(scope values.Scope) Completer {
	return Completer{scope: scope}
}

// Names returns the slice of names in scope.
func (c Completer) Names() []string {
	names := make([]string, 0, c.scope.Size())
	c.scope.Range(func(k string, v values.Value) {
		names = append(names, k)
	})
	sort.Strings(names)
	return names
}

// Value returns a value based on the expression name, if one exists.
func (c Completer) Value(name string) (values.Value, error) {
	v, ok := c.scope.Lookup(name)
	if !ok {
		return nil, errors.New("could not find value")
	}

	return v, nil
}

// FunctionNames returns the names of all function.
func (c Completer) FunctionNames() []string {
	funcs := []string{}

	c.scope.Range(func(name string, v values.Value) {
		if isFunction(v) {
			funcs = append(funcs, name)
		}
	})

	sort.Strings(funcs)

	return funcs
}

// FunctionSuggestion returns information needed for autocomplete suggestions for the function with the given name.
func (c Completer) FunctionSuggestion(name string) (FunctionSuggestion, error) {
	var s FunctionSuggestion

	v, err := c.Value(name)
	if err != nil {
		return s, err
	}

	if !isFunction(v) {
		return s, fmt.Errorf("name ( %s ) is not a function", name)
	}

	ft := v.Type()
	l, err := ft.NumArguments()
	if err != nil {
		return s, err
	}
	params := make(map[string]string, l)

	for i := 0; i < l; i++ {
		p, err := ft.Argument(i)
		if err != nil {
			return s, err
		}
		pt, err := p.TypeOf()
		if err != nil {
			return s, err
		}
		params[string(p.Name())] = pt.Nature().String()
	}

	s = FunctionSuggestion{
		Params: params,
	}

	return s, nil
}

func isFunction(v values.Value) bool {
	return v.Type().Nature() == semantic.Function
}
