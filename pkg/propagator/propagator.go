package propagator

import (
	"context"
	"net/http"
	"net/textproto"
	"reflect"

	"google.golang.org/grpc/metadata"

	"github.com/cortexproject/cortex/pkg/user"
)

type cortexPropagator struct {
}

func New() user.Propagator {
	return &cortexPropagator{}
}

func (*cortexPropagator) ExtractFromGRPCRequest(ctx context.Context) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx, user.ErrNoOrgID
	}

	orgIDs, ok := md[user.LowerOrgIDHeaderName]
	if !ok || len(orgIDs) < 1 {
		return ctx, user.ErrNoOrgID
	}

	return user.InjectTenantIDs(ctx, orgIDs), nil
}

func (*cortexPropagator) InjectIntoGRPCRequest(ctx context.Context) (context.Context, error) {
	orgIDs, err := user.Resolve.ExtractTenantIDs(ctx)
	if err != nil {
		return ctx, err
	}

	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(map[string]string{})
	}
	newCtx := ctx
	if existingIDs, ok := md[user.LowerOrgIDHeaderName]; ok {
		if !reflect.DeepEqual(orgIDs, existingIDs) {
			return ctx, user.ErrDifferentOrgIDPresent
		}
	} else {
		md = md.Copy()
		md[user.LowerOrgIDHeaderName] = orgIDs
		newCtx = metadata.NewOutgoingContext(ctx, md)
	}
	return newCtx, nil
}

func (*cortexPropagator) ExtractFromHTTPRequest(r *http.Request) (context.Context, error) {
	orgIDs, ok := r.Header[textproto.CanonicalMIMEHeaderKey(user.OrgIDHeaderName)]
	if !ok || len(orgIDs) == 0 {
		return r.Context(), user.ErrNoOrgID
	}
	return user.InjectTenantIDs(r.Context(), orgIDs), nil
}

func (*cortexPropagator) InjectIntoHTTPRequest(ctx context.Context, r *http.Request) error {
	orgIDs, err := user.Resolve.ExtractTenantIDs(ctx)
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
