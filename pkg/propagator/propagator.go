package propagator

import (
	"context"
	"net/http"
	"net/textproto"
	"reflect"

	"github.com/weaveworks/common/user"
	"google.golang.org/grpc/metadata"
)

type cortexPropagator struct {
}

type contextKey int

const (
	// Keys used in contexts to find the org or user ID
	tenantIDContextKey contextKey = 0
)

func New() user.Propagator {
	return &cortexPropagator{}
}

const (
	// TODO UPSTREAM EXPORT // LowerOrgIDHeaderName as gRPC / HTTP2.0 headers are lowercased.
	lowerOrgIDHeaderName = "x-scope-orgid"
)

func ExtractTenantIDs(ctx context.Context) ([]string, error) {
	orgIDs, ok := ctx.Value(tenantIDContextKey).([]string)
	if !ok || len(orgIDs) == 0 {
		return nil, user.ErrNoOrgID
	}
	return orgIDs, nil
}

func InjectTenantIDs(ctx context.Context, orgIDs []string) context.Context {
	// TODO: This should be a more generic one
	ctx = user.InjectUserID(ctx, orgIDs[0])
	return context.WithValue(ctx, tenantIDContextKey, orgIDs)
}

func (*cortexPropagator) ExtractFromGRPCRequest(ctx context.Context) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx, user.ErrNoOrgID
	}

	orgIDs, ok := md[lowerOrgIDHeaderName]
	if !ok || len(orgIDs) < 1 {
		return ctx, user.ErrNoOrgID
	}

	return InjectTenantIDs(ctx, orgIDs), nil
}

func (*cortexPropagator) InjectIntoGRPCRequest(ctx context.Context) (context.Context, error) {
	orgIDs, err := ExtractTenantIDs(ctx)
	if err != nil {
		return ctx, err
	}

	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(map[string]string{})
	}
	newCtx := ctx
	if existingIDs, ok := md[lowerOrgIDHeaderName]; ok {
		if !reflect.DeepEqual(orgIDs, existingIDs) {
			return ctx, user.ErrTooManyOrgIDs
		}
	} else {
		md = md.Copy()
		md[lowerOrgIDHeaderName] = orgIDs
		newCtx = metadata.NewOutgoingContext(ctx, md)
	}
	return newCtx, nil
}

func (*cortexPropagator) ExtractFromHTTPRequest(r *http.Request) (context.Context, error) {
	orgIDs, ok := r.Header[textproto.CanonicalMIMEHeaderKey(user.OrgIDHeaderName)]
	if !ok || len(orgIDs) == 0 {
		return r.Context(), user.ErrNoOrgID
	}
	return InjectTenantIDs(r.Context(), orgIDs), nil
}

func (*cortexPropagator) InjectIntoHTTPRequest(ctx context.Context, r *http.Request) error {
	orgIDs, err := ExtractTenantIDs(ctx)
	if err != nil {
		return err
	}

	existingIDs := r.Header[textproto.CanonicalMIMEHeaderKey(user.OrgIDHeaderName)]
	if len(existingIDs) > 0 && !reflect.DeepEqual(existingIDs, orgIDs) {
		return user.ErrDifferentOrgIDPresent
	}

	for _, orgID := range orgIDs {
		r.Header.Add(user.OrgIDHeaderName, orgID)
	}
	return nil
}
