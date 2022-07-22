package api

import (
	"context"
	"net/http"
	"regexp"

	"github.com/cortexproject/cortex/pkg/chunk/purger"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/cortexproject/cortex/pkg/tenant"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/weaveworks/common/middleware"
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

// HTTPHeaderMiddleware adds specified HTTPHeaders to the request context
type HTTPHeaderMiddleware struct {
	TargetHeaders []string
}

// NewHttpHeaderMiddleware creates a new hTTPHeaderMiddleware given a string containing headers delimited by ~ (whitespaces in headers ignored)
func NewHttpHeaderMiddleware(unparsedTargetHeaders string) HTTPHeaderMiddleware {
	removeWhitespaceRegex, err := regexp.Compile(`\s`)
	if err != nil {
		util_log.Logger.Log("WhitespaceRegexCompilationStatus", "Failed")
		return HTTPHeaderMiddleware{TargetHeaders: nil}
	}
	headersWithNoWhitespace := removeWhitespaceRegex.ReplaceAllString(unparsedTargetHeaders, "")

	splitRegex, err2 := regexp.Compile(`~`)
	if err2 != nil {
		util_log.Logger.Log("SplitRegexCompilationStatus", "Failed")
		return HTTPHeaderMiddleware{TargetHeaders: nil}
	}
	splitHeaders := splitRegex.Split(headersWithNoWhitespace, -1)
	return HTTPHeaderMiddleware{TargetHeaders: splitHeaders}

}

// InjectRequestIdIntoHTTPRequest injects specified HTTPHeaders into the request context
func (h HTTPHeaderMiddleware) InjectRequestIdIntoHTTPRequest(r *http.Request) context.Context {
	// Check to make sure that Headers have not already been injected
	testing, ok := r.Context().Value(util_log.RequestTargetsContextKey).([]string)
	if ok && h.TargetHeaders != nil && testing[0] == h.TargetHeaders[0] {
		return r.Context()
	}
	headerContents := make([]string, len(h.TargetHeaders))
	for index, target := range h.TargetHeaders {
		headerContents[index] = r.Header.Get(target)
	}
	ctx := r.Context()
	ctx = context.WithValue(ctx, util_log.RequestTargetsContextKey, h.TargetHeaders)
	ctx = context.WithValue(ctx, util_log.RequestValuesContextKey, headerContents)
	return ctx
}

// Wrap implements Middleware
func (h HTTPHeaderMiddleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := h.InjectRequestIdIntoHTTPRequest(r)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
