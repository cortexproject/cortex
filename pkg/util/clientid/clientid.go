// Package clientid extracts the optional, trusted X-Scope-ClientID header
// introduced by the client-identity ingestion/query rate limiting proposals
// (docs/proposals/client-identity-ingestion-rate-limiting.md and
// docs/proposals/client-identity-query-rate-limiting.md).
//
// X-Scope-ClientID is trusted exactly the same way weaveworks/common/user's
// X-Scope-OrgID already is: Cortex assumes it is set by a trusted
// gateway/proxy in front of it, not supplied directly by an untrusted
// caller. Unlike OrgID, ClientID is presence-optional -- an absent header is
// not an error, it just means the request isn't subject to the additional
// per-client checks this package's callers implement.
package clientid

import (
	"context"
	"net/http"
	"strings"

	"google.golang.org/grpc/metadata"
)

// HeaderName is the trusted header carrying the opaque client identity.
// Deliberately reuses the "X-Scope-" prefix from X-Scope-OrgID so the two
// headers read as an obviously related pair to an operator.
const HeaderName = "X-Scope-ClientID"

// ExtractFromHTTPRequest returns the client ID from the request headers, or
// "" if absent. An absent header is never an error: presence gates whether
// the additional per-client checks apply at all, it does not gate whether
// the request itself is valid.
func ExtractFromHTTPRequest(r *http.Request) string {
	return r.Header.Get(HeaderName)
}

// ExtractFromGRPCContext returns the client ID from the incoming gRPC
// metadata, or "" if absent. It is the equivalent of ExtractFromHTTPRequest
// for write paths that enter Cortex over gRPC (OTLP gRPC); gRPC metadata keys
// are always lowercase.
func ExtractFromGRPCContext(ctx context.Context) string {
	if values := metadata.ValueFromIncomingContext(ctx, strings.ToLower(HeaderName)); len(values) > 0 {
		return values[0]
	}
	return ""
}

type contextKey int

const clientIDContextKey contextKey = 0

// InjectIntoContext threads the extracted client ID through the request the
// same way pkg/util.AddSourceIPsToOutgoingContext threads source IPs today:
// read once at the HTTP entrypoint (see pkg/util/push.Handler), then read
// back wherever it's needed (the distributor's Push, the query-frontend's
// tripperware) without re-parsing headers. Unlike source IPs, this never
// needs to cross a gRPC hop to a downstream component (enforcement happens
// entirely within the component that first saw the header), so a plain
// context value is enough -- no gRPC outgoing/incoming metadata dance.
func InjectIntoContext(ctx context.Context, clientID string) context.Context {
	if clientID == "" {
		return ctx
	}
	return context.WithValue(ctx, clientIDContextKey, clientID)
}

// FromContext returns the client ID previously injected by InjectIntoContext,
// or "" if none was ever injected (i.e. the request had no X-Scope-ClientID
// header).
func FromContext(ctx context.Context) string {
	clientID, _ := ctx.Value(clientIDContextKey).(string)
	return clientID
}
