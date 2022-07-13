package util

import (
	"context"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/log/level"
	"net/http"
	"os"
)

// HTTPLogger adjusts HTTPHeaders to allow for logging
type HTTPLogger struct {
	Header       http.Header
	TargetHeader string
}

// InjectTargetHeadersIntoHTTPRequest injects the orgID from the context into the request headers.
func (h HTTPLogger) InjectTargetHeadersIntoHTTPRequest(r *http.Request) context.Context {
	// Temp code for testing purposes
	w := log.NewSyncWriter(os.Stderr)
	logger := log.NewLogfmtLogger(w)

	existingID := r.Header.Get("RequestID")
	if existingID != "" && existingID != h.TargetHeader {
		return r.Context()
	}
	ctx := r.Context()
	ctx = context.WithValue(ctx, "RequestID", r.Header.Get(h.TargetHeader))
	level.Warn(logger).Log("requestID", ctx.Value("RequestID"))
	return ctx
}

// Wrap implements Middleware
func (h HTTPLogger) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := h.InjectTargetHeadersIntoHTTPRequest(r)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

//// ExtractHeadersFromHTTPRequest Extracts the specified headers from HTTP request and returns them, along with a context with those values embedded
//func ExtractHeadersFromHTTPRequest(r *http.Request, target string) (string, context.Context, error) {
//	reqID := r.Header.Get(target)
//	if reqID == "" {
//		return "", r.Context(), commonErrors.Error("No headers to extract")
//	}
//	return reqID, InjectReqID(r.Context(), reqID), nil
//}
//
//func InjectReqID(ctx context.Context, reqID string) context.Context {
//	return context.WithValue(ctx, "RequestID", reqID)
//}
//var Err = errors.New("HTTP prefix should be empty or start with /")

// HTTPLogging is supposed to modify http headers
//var HTTPLogging = Func(func(next http.Handler) http.Handler {
//	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
//		_, ctx, err := ExtractHeadersFromHTTPRequest()
//		if err != nil {
//			http.Error(w, err.Error(), http.StatusUnauthorized)
//			return
//		}
//
//		next.ServeHTTP(w, r.WithContext(ctx))
//	})
//})
