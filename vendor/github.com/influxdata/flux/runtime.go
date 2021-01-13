package flux

import (
	"context"
	"time"

	"github.com/influxdata/flux/interpreter"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/values"
)

// Runtime encapsulates the operations supported by the flux runtime.
type Runtime interface {
	// Parse parses a Flux script and produces a handle to an AST.
	Parse(flux string) (ASTHandle, error)

	// JSONToHandle takes JSON data and returns an AST handle.
	JSONToHandle(json []byte) (ASTHandle, error)

	// MargePackages removes all the files from src and appends them to the list
	// of files in dst.
	MergePackages(dst, src ASTHandle) error

	// Eval accepts a Flux AST and evaluates it to produce a set of side effects (as a slice of values) and a scope.
	Eval(ctx context.Context, astPkg ASTHandle, es interpreter.ExecOptsConfig, opts ...ScopeMutator) ([]interpreter.SideEffect, values.Scope, error)

	// IsPreludePackage will return if the named package is part
	// of the prelude for this runtime.
	IsPreludePackage(pkg string) bool

	// LookupBuiltinType returns the type of the builtin value for a given
	// Flux stdlib package. Returns an error if lookup fails.
	LookupBuiltinType(pkg, name string) (semantic.MonoType, error)
}

// ASTHandle is an opaque type that represents an abstract syntax tree.
type ASTHandle interface {
	// ASTHandle is a no-op method whose purpose is to avoid types unintentionally
	// implementing this interface.
	ASTHandle()

	// GetError will return the first error encountered when parsing Flux source code,
	// if any.
	GetError() error
}

// ScopeMutator is any function that mutates the scope of an identifier.
type ScopeMutator = func(r Runtime, scope values.Scope)

// SetOption returns a func that adds a var binding to a scope.
func SetOption(pkg, name string, fn func(r Runtime) values.Value) ScopeMutator {
	return func(r Runtime, scope values.Scope) {
		v := fn(r)
		p, ok := scope.Lookup(pkg)
		if ok {
			if p, ok := p.(values.Package); ok {
				values.SetOption(p, name, v)
			}
		} else if r.IsPreludePackage(pkg) {
			opt, ok := scope.Lookup(name)
			if ok {
				if opt, ok := opt.(*values.Option); ok {
					opt.Value = v
				}
			}
		}
	}
}

// SetNowOption returns a ScopeMutator that sets the `now` option to the given time.
func SetNowOption(now time.Time) ScopeMutator {
	return SetOption(interpreter.NowPkg, interpreter.NowOption, generateNowFunc(now))
}

func generateNowFunc(now time.Time) func(r Runtime) values.Value {
	return func(r Runtime) values.Value {
		timeVal := values.NewTime(values.ConvertTime(now))
		ftype, err := r.LookupBuiltinType("universe", "now")
		if err != nil {
			panic(err)
		}
		call := func(ctx context.Context, args values.Object) (values.Value, error) {
			return timeVal, nil
		}
		return values.NewFunction(interpreter.NowOption, ftype, call, false)
	}
}
