package log

import (
	"context"

	"github.com/go-kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"go.opentelemetry.io/otel/trace"

	"github.com/cortexproject/cortex/pkg/tenant"
)

// WithUserID returns a Logger that has information about the current user in
// its details.
func WithUserID(userID string, l log.Logger) log.Logger {
	// See note in WithContext.
	return log.With(l, "org_id", userID)
}

// WithTraceID returns a Logger that has information about the traceID in
// its details.
func WithTraceID(traceID string, l log.Logger) log.Logger {
	// See note in WithContext.
	return log.With(l, "traceID", traceID)
}

// WithContext returns a Logger that has information about the current user in
// its details.
//
// e.g.
//
//	log := util.WithContext(ctx)
//	log.Errorf("Could not chunk chunks: %v", err)
func WithContext(ctx context.Context, l log.Logger) log.Logger {
	l = HeadersFromContext(ctx, l)

	// Weaveworks uses "orgs" and "orgID" to represent Cortex users,
	// even though the code-base generally uses `userID` to refer to the same thing.
	userID, err := tenant.TenantID(ctx)
	if err == nil {
		l = WithUserID(userID, l)
	}

	traceID, ok := ExtractSampledTraceID(ctx)
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

// HeadersFromContext enables the logging of specified HTTP Headers that have been added to a context
func HeadersFromContext(ctx context.Context, l log.Logger) log.Logger {
	headerContentsMap := HeaderMapFromContext(ctx)
	for header, contents := range headerContentsMap {
		l = log.With(l, header, contents)
	}
	return l
}

// ExtractSampledTraceID gets traceID and whether the trace is samples or not.
func ExtractSampledTraceID(ctx context.Context) (string, bool) {
	sp := opentracing.SpanFromContext(ctx)
	if sp == nil {
		return "", false
	}
	sctx, ok := sp.Context().(jaeger.SpanContext)
	if !ok {
		// If OpenTracing span not found, try OTEL.
		span := trace.SpanFromContext(ctx)
		if span != nil {
			return span.SpanContext().TraceID().String(), span.SpanContext().IsSampled()
		}
		return "", false
	}

	return sctx.TraceID().String(), sctx.IsSampled()
}
