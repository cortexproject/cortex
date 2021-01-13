package values

import (
	"fmt"
)

type Scope interface {
	// Lookup a name in the scope.
	Lookup(name string) (Value, bool)

	// LocalLookup a name in current scope only.
	LocalLookup(name string) (Value, bool)

	// Set binds a variable in the current scope.
	Set(name string, v Value)

	// Nest creates a new scope by nesting the current scope.
	// If the passed in object is not nil, its values will be added to the new nested scope.
	Nest(Object) Scope

	// Pop returns the parent of the current scope.
	Pop() Scope

	// Size is the number of visible names in scope.
	Size() int

	// Range iterates over all variable bindings in scope applying f.
	Range(f func(k string, v Value))

	// LocalRange iterates over all variable bindings only in the current scope.
	LocalRange(f func(k string, v Value))

	// SetReturn binds the return value of the scope.
	SetReturn(Value)

	// Return reports the bound return value of the scope.
	Return() Value

	// Copy creates a deep copy of the scope, values are not copied.
	// Copy preserves the nesting structure.
	Copy() Scope
}

type scope struct {
	parent      Scope
	values      map[string]Value
	returnValue Value
}

// NewScope creates a new empty scope with no parent.
func NewScope() Scope {
	return &scope{
		values: make(map[string]Value),
	}
}

// NewNestedScope creates a new scope with bindings from obj and a parent.
func NewNestedScope(parent Scope, obj Object) Scope {
	var values map[string]Value
	if obj != nil {
		values = make(map[string]Value, obj.Len())
		obj.Range(func(name string, v Value) {
			values[name] = v
		})
	}
	return &scope{
		parent: parent,
		values: values,
	}
}

func (s *scope) Lookup(name string) (Value, bool) {
	v, ok := s.values[name]
	if !ok && s.parent != nil {
		return s.parent.Lookup(name)
	}
	return v, ok
}
func (s *scope) LocalLookup(name string) (Value, bool) {
	v, ok := s.values[name]
	return v, ok
}

func (s *scope) Set(name string, v Value) {
	if s.values == nil {
		s.values = make(map[string]Value)
	}
	s.values[name] = v
}

func (s *scope) Nest(obj Object) Scope {
	return NewNestedScope(s, obj)
}

func (s *scope) Pop() Scope {
	return s.parent
}

func (s *scope) Size() int {
	if s.parent == nil {
		return len(s.values)
	}
	return len(s.values) + s.parent.Size()
}

func (s *scope) Range(f func(k string, v Value)) {
	s.LocalRange(f)
	if s.parent != nil {
		s.parent.Range(f)
	}
}

func (s *scope) LocalRange(f func(k string, v Value)) {
	for k, v := range s.values {
		f(k, v)
	}
}

func (s *scope) SetReturn(v Value) {
	s.returnValue = v
}

func (s *scope) Return() Value {
	return s.returnValue
}

func (s *scope) Copy() Scope {
	var pc Scope
	if s.parent != nil {
		pc = s.parent.Copy()
	}
	ns := NewNestedScope(pc, nil)
	s.LocalRange(func(k string, v Value) {
		ns.Set(k, v)
	})
	return ns
}

// FormattedScope produces a fmt.Formatter for pretty printing a scope.
func FormattedScope(scope Scope) fmt.Formatter {
	return scopeFormatter{scope}
}

type scopeFormatter struct {
	scope Scope
}

func (s scopeFormatter) Format(state fmt.State, _ rune) {
	state.Write([]byte("["))
	for scope := s.scope; scope != nil; scope = scope.Pop() {
		state.Write([]byte("{"))
		j := 0
		scope.LocalRange(func(k string, v Value) {
			if j != 0 {
				state.Write([]byte(", "))
			}
			fmt.Fprintf(state, "%s = %v", k, v)
			j++
		})
		state.Write([]byte("} -> "))
	}
	state.Write([]byte("nil ]"))
}
