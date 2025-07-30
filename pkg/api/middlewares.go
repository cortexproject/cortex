package api

import (
	"net/http"

	"github.com/google/uuid"

	"github.com/cortexproject/cortex/pkg/util/requestmeta"
)

// HTTPHeaderMiddleware adds specified HTTPHeaders to the request context
type HTTPHeaderMiddleware struct {
	TargetHeaders   []string
	RequestIdHeader string
}

// injectRequestContext injects request related metadata into the request context
func (h HTTPHeaderMiddleware) injectRequestContext(r *http.Request) *http.Request {
	requestContextMap := make(map[string]string)

	// Check to make sure that request context have not already been injected
	checkMapInContext := requestmeta.MapFromContext(r.Context())
	if checkMapInContext != nil {
		return r
	}

	for _, target := range h.TargetHeaders {
		contents := r.Header.Get(target)
		if contents != "" {
			requestContextMap[target] = contents
		}
	}
	requestContextMap[requestmeta.LoggingHeadersKey] = requestmeta.LoggingHeaderKeysToString(h.TargetHeaders)

	reqId := r.Header.Get(h.RequestIdHeader)
	if reqId == "" {
		reqId = uuid.NewString()
	}
	requestContextMap[requestmeta.RequestIdKey] = reqId

	ctx := requestmeta.ContextWithRequestMetadataMap(r.Context(), requestContextMap)
	return r.WithContext(ctx)
}

// Wrap implements Middleware
func (h HTTPHeaderMiddleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = h.injectRequestContext(r)
		next.ServeHTTP(w, r)
	})
}
