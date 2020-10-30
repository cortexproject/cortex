package user

import (
	"context"

	"github.com/weaveworks/common/user"
)

const (
	// TODO UPSTREAM EXPORT // LowerOrgIDHeaderName as gRPC / HTTP2.0 headers are lowercased.
	LowerOrgIDHeaderName = "x-scope-orgid"
	OrgIDHeaderName      = user.OrgIDHeaderName
)

type Propagator = user.Propagator

var (
	ErrNoOrgID               = user.ErrNoOrgID
	ErrDifferentOrgIDPresent = user.ErrDifferentOrgIDPresent
)

var Resolve = NewResolver()

type Resolver interface {
	// UserID identifies a user of Cortex uniquely, e.g. it's limits, queue and metrics are derived from it
	UserID(ctx context.Context) (string, error)
	// TenantIDs returns all tenant IDs for a particular query
	TenantIDs(ctx context.Context) ([]string, error)
}

type defaultResolver struct{}

func NewResolver() *defaultResolver {
	return &defaultResolver{}
}

func (r *defaultResolver) UserID(ctx context.Context) (string, error) {
	tenantIDs, err := r.ExtractTenantIDs(ctx)
	if err != nil {
		return "", err
	}
	return tenantIDs[0], nil
}

func (*defaultResolver) ExtractTenantIDs(ctx context.Context) ([]string, error) {
	orgIDs, ok := ctx.Value(tenantIDContextKey).([]string)
	if !ok || len(orgIDs) == 0 {
		return nil, user.ErrNoOrgID
	}
	return orgIDs, nil
}

type contextKey int

const (
	// Keys used in contexts to find the org or user ID
	tenantIDContextKey contextKey = 0
)

func InjectTenantIDs(ctx context.Context, orgIDs []string) context.Context {
	// TODO: This should be a more generic one
	ctx = user.InjectUserID(ctx, orgIDs[0])
	return context.WithValue(ctx, tenantIDContextKey, orgIDs)
}
