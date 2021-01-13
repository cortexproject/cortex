package flux

import (
	"context"

	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/internal/errors"
	"github.com/influxdata/flux/memory"
)

// Compiler produces a specification for the query.
type Compiler interface {
	// Compile produces a specification for the query.
	Compile(ctx context.Context, runtime Runtime) (Program, error)
	CompilerType() CompilerType
}

// CompilerType is the name of a query compiler.
type CompilerType string
type CreateCompiler func() Compiler
type CompilerMappings map[CompilerType]CreateCompiler

func (m CompilerMappings) Add(t CompilerType, c CreateCompiler) error {
	if _, ok := m[t]; ok {
		return errors.Newf(codes.Internal, "duplicate compiler mapping for %q", t)
	}
	m[t] = c
	return nil
}

// Program defines a Flux script which has been compiled.
type Program interface {
	// Start begins execution of the program and returns immediately.
	// As results are produced they arrive on the channel.
	// The program is finished once the result channel is closed and all results have been consumed.
	Start(context.Context, *memory.Allocator) (Query, error)
}
