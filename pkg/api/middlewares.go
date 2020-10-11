package api

import (
	"net/http"

	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/chunk/purger"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
)

// Latest Prometheus requires r.RemoteAddr to be set to addr:port, otherwise it reject the request.
// Requests to Querier sometimes doesn't have that (if they are fetched from Query-Frontend).
// Prometheus uses this when logging queries to QueryLogger, but Cortex doesn't call engine.SetQueryLogger to set one.
//
// Can be removed when (if) https://github.com/prometheus/prometheus/pull/6840 is merged.
func fakeRemoteAddr() middleware.Interface {
	return middleware.Func(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.RemoteAddr == "" {
				r.RemoteAddr = "127.0.0.1:8888"
			}
			next.ServeHTTP(w, r)
		})
	})
}

// middleware for setting cache gen header to let consumer of response know all previous responses could be invalid due to delete operation
func getHTTPCacheGenNumberHeaderSetterMiddleware(cacheGenNumbersLoader *purger.TombstonesLoader) middleware.Interface {
	return middleware.Func(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			userID, err := user.ExtractOrgID(r.Context())
			if err != nil {
				http.Error(w, err.Error(), http.StatusUnauthorized)
				return
			}

			cacheGenNumber := cacheGenNumbersLoader.GetResultsCacheGenNumber(userID)

			w.Header().Set(queryrange.ResultsCacheGenNumberHeaderName, cacheGenNumber)
			next.ServeHTTP(w, r)
		})
	})
}
