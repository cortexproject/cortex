package middleware

import (
	"net/http"

	"github.com/weaveworks/common/user"
)

func (with *withPropagator) AuthenticateUser() Func {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, err := with.propagator.ExtractFromHTTPRequest(r)
			if err != nil {
				http.Error(w, err.Error(), http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// AuthenticateUser propagates the user ID from HTTP headers back to the request's context.
var AuthenticateUser = WithPropagator(user.NewOrgIDPropagator()).AuthenticateUser()
