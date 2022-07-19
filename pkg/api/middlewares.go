package api

import (
	"context"
	"github.com/cortexproject/cortex/pkg/chunk/purger"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/cortexproject/cortex/pkg/tenant"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/weaveworks/common/middleware"
	"net/http"
)

// middleware for setting cache gen header to let consumer of response know all previous responses could be invalid due to delete operation
func getHTTPCacheGenNumberHeaderSetterMiddleware(cacheGenNumbersLoader *purger.TombstonesLoader) middleware.Interface {
	return middleware.Func(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tenantIDs, err := tenant.TenantIDs(r.Context())
			if err != nil {
				http.Error(w, err.Error(), http.StatusUnauthorized)
				return
			}

			cacheGenNumber := cacheGenNumbersLoader.GetResultsCacheGenNumber(tenantIDs)

			w.Header().Set(queryrange.ResultsCacheGenNumberHeaderName, cacheGenNumber)
			next.ServeHTTP(w, r)
		})
	})
}

// HTTPHeaderMiddleware adds specified HTTPHeader to the request context
type HTTPHeaderMiddleware struct {
	TargetHeader string
}

// InjectRequestIdIntoHTTPRequest injects specified RequestID into the request context
func (h HTTPHeaderMiddleware) InjectRequestIdIntoHTTPRequest(r *http.Request) context.Context {

	//TODO comments
	//TODO See if next 3 lines needed
	existingID := r.Header.Get("RequestID")
	if existingID != "" && existingID != h.TargetHeader {
		return r.Context()
	}
	ctx := r.Context()
	ctx = context.WithValue(ctx, util_log.RequestIdContextKey, r.Header.Get(h.TargetHeader))
	return ctx
}

// Wrap implements Middleware
func (h HTTPHeaderMiddleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := h.InjectRequestIdIntoHTTPRequest(r)
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
