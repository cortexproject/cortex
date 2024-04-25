package api

import (
	"context"
	"net/http"

	util_log "github.com/cortexproject/cortex/pkg/util/log"
)

// HTTPHeaderMiddleware adds specified HTTPHeaders to the request context
type HTTPHeaderMiddleware struct {
	TargetHeaders []string
}

// InjectTargetHeadersIntoHTTPRequest injects specified HTTPHeaders into the request context
func (h HTTPHeaderMiddleware) InjectTargetHeadersIntoHTTPRequest(r *http.Request) context.Context {
	headerMap := make(map[string]string)

	// Check to make sure that Headers have not already been injected
	checkMapInContext := util_log.HeaderMapFromContext(r.Context())
	if checkMapInContext != nil {
		return r.Context()
	}

	for _, target := range h.TargetHeaders {
		contents := r.Header.Get(target)
		if contents != "" {
			headerMap[target] = contents
		}
	}
	return util_log.ContextWithHeaderMap(r.Context(), headerMap)
}

// Wrap implements Middleware
func (h HTTPHeaderMiddleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := h.InjectTargetHeadersIntoHTTPRequest(r)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
