package log

import (
	"context"

	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/go-kit/log"
	kitlog "github.com/go-kit/log"
	"github.com/weaveworks/common/tracing"
)

// WithUserID returns a Logger that has information about the current user in
// its details.
func WithUserID(userID string, l kitlog.Logger) kitlog.Logger {
	// See note in WithContext.
	return kitlog.With(l, "org_id", userID)
}

// WithTraceID returns a Logger that has information about the traceID in
// its details.
func WithTraceID(traceID string, l kitlog.Logger) kitlog.Logger {
	// See note in WithContext.
	return kitlog.With(l, "traceID", traceID)
}

// WithContext returns a Logger that has information about the current user in
// its details.
//
// e.g.
//   log := util.WithContext(ctx)
//   log.Errorf("Could not chunk chunks: %v", err)
func WithContext(ctx context.Context, l kitlog.Logger) kitlog.Logger {
	// Weaveworks uses "orgs" and "orgID" to represent Cortex users,
	// even though the code-base generally uses `userID` to refer to the same thing.

	//TODO comments
	reqId, ok := RequestID(ctx)
	if ok {
		l = kitlog.With(l, "requestID", reqId)
	}
	userID, err := tenant.TenantID(ctx)
	if err == nil {
		l = WithUserID(userID, l)
	}
	traceID, ok := tracing.ExtractSampledTraceID(ctx)
	if !ok {
		return l
	}

	return WithTraceID(traceID, l)
}

// WithSourceIPs returns a Logger that has information about the source IPs in
// its details.
func WithSourceIPs(sourceIPs string, l log.Logger) log.Logger {
	return log.With(l, "sourceIPs", sourceIPs)
}

type contextKey int

const (
	RequestIdContextKey contextKey = 0
)

func RequestID(ctx context.Context) (string, bool) {
	id, worked := ctx.Value(RequestIdContextKey).(string)
	return id, worked
}

//
//// Testing RequestID Logging
//func WithRequestID(ctx context.Context, l kitlog.Logger) kitlog.Logger {
//	l = WithContext(ctx, l)
//	reqId, ok := RequestID(ctx)
//	if ok {
//		l = kitlog.With(l, "requestID", reqId)
//	}
//	return l
//}
//
//func RequestIDFromHTTP(ctx context.Context, req *http.Request, header string) context.Context {
//	if header == "" {
//		return ctx
//	}
//	id := req.Header.Get(header)
//	if id == "" {
//		return ctx
//	}
//	return context.WithValue(ctx, requestIdContextKey, id)
//}
