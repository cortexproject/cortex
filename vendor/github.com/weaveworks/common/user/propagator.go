package user

import (
	"net/http"

	"golang.org/x/net/context"
	"google.golang.org/grpc/metadata"
)

type Propagator interface {
	ExtractFromHTTPRequest(r *http.Request) (context.Context, error)
	InjectIntoHTTPRequest(ctx context.Context, r *http.Request) error
	ExtractFromGRPCRequest(ctx context.Context) (context.Context, error)
	InjectIntoGRPCRequest(ctx context.Context) (context.Context, error)
}

func NewOrgIDPropagator() *orgIDPropagator {
	return &orgIDPropagator{}
}

type orgIDPropagator struct{}

func (_ orgIDPropagator) ExtractFromGRPCRequest(ctx context.Context) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx, ErrNoOrgID
	}

	orgIDs, ok := md[lowerOrgIDHeaderName]
	if !ok || len(orgIDs) != 1 {
		return ctx, ErrNoOrgID
	}

	return InjectOrgID(ctx, orgIDs[0]), nil
}

func (_ orgIDPropagator) InjectIntoGRPCRequest(ctx context.Context) (context.Context, error) {
	orgID, err := ExtractOrgID(ctx)
	if err != nil {
		return ctx, err
	}

	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(map[string]string{})
	}
	newCtx := ctx
	if orgIDs, ok := md[lowerOrgIDHeaderName]; ok {
		if len(orgIDs) == 1 {
			if orgIDs[0] != orgID {
				return ctx, ErrDifferentOrgIDPresent
			}
		} else {
			return ctx, ErrTooManyOrgIDs
		}
	} else {
		md = md.Copy()
		md[lowerOrgIDHeaderName] = []string{orgID}
		newCtx = metadata.NewOutgoingContext(ctx, md)
	}

	return newCtx, nil
}

// ExtractOrgIDFromHTTPRequest extracts the org ID from the request headers and returns
// the org ID and a context with the org ID embedded.
func (_ orgIDPropagator) ExtractFromHTTPRequest(r *http.Request) (context.Context, error) {
	orgID := r.Header.Get(OrgIDHeaderName)
	if orgID == "" {
		return r.Context(), ErrNoOrgID
	}
	return InjectOrgID(r.Context(), orgID), nil
}

// InjectOrgIDIntoHTTPRequest injects the orgID from the context into the request headers.
func (_ orgIDPropagator) InjectIntoHTTPRequest(ctx context.Context, r *http.Request) error {
	orgID, err := ExtractOrgID(ctx)
	if err != nil {
		return err
	}
	existingID := r.Header.Get(OrgIDHeaderName)
	if existingID != "" && existingID != orgID {
		return ErrDifferentOrgIDPresent
	}
	r.Header.Set(OrgIDHeaderName, orgID)
	return nil
}
