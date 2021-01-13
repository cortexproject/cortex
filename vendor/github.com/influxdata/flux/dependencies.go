package flux

import (
	"context"

	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/dependencies/filesystem"
	"github.com/influxdata/flux/dependencies/http"
	"github.com/influxdata/flux/dependencies/secret"
	"github.com/influxdata/flux/dependencies/url"
	"github.com/influxdata/flux/internal/errors"
)

var _ Dependencies = (*Deps)(nil)

// Dependency is an interface that must be implemented by every injectable dependency.
// On Inject, the dependency is injected into the context and the resulting one is returned.
// Every dependency must provide a function to extract it from the context.
type Dependency interface {
	Inject(ctx context.Context) context.Context
}

type key int

const dependenciesKey key = iota

type Dependencies interface {
	Dependency
	HTTPClient() (http.Client, error)
	FilesystemService() (filesystem.Service, error)
	SecretService() (secret.Service, error)
	URLValidator() (url.Validator, error)
}

// Deps implements Dependencies.
// Any deps which are nil will produce an explicit error.
type Deps struct {
	Deps WrappedDeps
}

type WrappedDeps struct {
	HTTPClient        http.Client
	FilesystemService filesystem.Service
	SecretService     secret.Service
	URLValidator      url.Validator
}

func (d Deps) HTTPClient() (http.Client, error) {
	if d.Deps.HTTPClient != nil {
		return d.Deps.HTTPClient, nil
	}
	return nil, errors.New(codes.Unimplemented, "http client uninitialized in dependencies")
}

func (d Deps) FilesystemService() (filesystem.Service, error) {
	if d.Deps.FilesystemService != nil {
		return d.Deps.FilesystemService, nil
	}
	return nil, errors.New(codes.Unimplemented, "filesystem service uninitialized in dependencies")
}

func (d Deps) SecretService() (secret.Service, error) {
	if d.Deps.SecretService != nil {
		return d.Deps.SecretService, nil
	}
	return nil, errors.New(codes.Unimplemented, "secret service uninitialized in dependencies")
}

func (d Deps) URLValidator() (url.Validator, error) {
	if d.Deps.URLValidator != nil {
		return d.Deps.URLValidator, nil
	}
	return nil, errors.New(codes.Unimplemented, "url validator uninitialized in dependencies")
}

func (d Deps) Inject(ctx context.Context) context.Context {
	return context.WithValue(ctx, dependenciesKey, d)
}

func GetDependencies(ctx context.Context) Dependencies {
	deps := ctx.Value(dependenciesKey)
	if deps == nil {
		return NewEmptyDependencies()
	}
	return deps.(Dependencies)
}

// NewDefaultDependencies produces a set of dependencies.
// Not all dependencies have valid defaults and will not be set.
func NewDefaultDependencies() Deps {
	validator := url.PassValidator{}
	return Deps{
		Deps: WrappedDeps{
			HTTPClient: http.NewLimitedDefaultClient(validator),
			// Default to having no filesystem, no secrets, and no url validation (always pass).
			FilesystemService: nil,
			SecretService:     secret.EmptySecretService{},
			URLValidator:      validator,
		},
	}
}

// NewEmptyDependencies produces an empty set of dependencies.
// Accessing any dependency will result in an error.
func NewEmptyDependencies() Dependencies {
	return Deps{}
}
