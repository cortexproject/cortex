package user

import (
	"golang.org/x/net/context"
)

// ExtractFromGRPCRequest extracts the orgID ID from the request metadata and returns
// the org ID and a context with the org ID injected.
func ExtractFromGRPCRequest(ctx context.Context) (string, context.Context, error) {
	ctx, err := NewOrgIDPropagator().ExtractFromGRPCRequest(ctx)
	if err != nil {
		return "", ctx, err
	}

	orgID, err := ExtractOrgID(ctx)

	return orgID, ctx, err
}

// InjectIntoGRPCRequest injects the orgID from the context into the request metadata.
func InjectIntoGRPCRequest(ctx context.Context) (context.Context, error) {
	return NewOrgIDPropagator().InjectIntoGRPCRequest(ctx)
}
