// +build go1.7

package nethttp

import (
	"net/http"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

type statusCodeTracker struct {
	http.ResponseWriter
	status int
}

func (w *statusCodeTracker) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

// Middleware wraps an http.Handler and traces incoming requests.
// Additionally, it adds the span to the request's context.
//
// Example:
// 	http.ListenAndServe("localhost:80", nethttp.Middleware(t1, http.DefaultServeMux))
func Middleware(tr opentracing.Tracer, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		ctx, _ := tr.Extract(opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(r.Header))
		sp := tr.StartSpan("HTTP "+r.Method, ext.RPCServerOption(ctx))
		ext.HTTPMethod.Set(sp, r.Method)
		ext.HTTPUrl.Set(sp, r.URL.String())
		ext.Component.Set(sp, "net/http")
		w = &statusCodeTracker{w, 200}
		r = r.WithContext(opentracing.ContextWithSpan(r.Context(), sp))

		h.ServeHTTP(w, r)

		ext.HTTPStatusCode.Set(sp, uint16(w.(*statusCodeTracker).status))
		sp.Finish()
	}
	return http.HandlerFunc(fn)
}
