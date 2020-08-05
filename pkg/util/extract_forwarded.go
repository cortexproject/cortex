package util

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

type source string

var sourceKey source

// GetSource extracts the X-FORWARDED-FOR header from the given HTTP request
// and returns a string with it and the remote address
func GetSource(req *http.Request) string {
	fwd := req.Header.Get("X-FORWARDED-FOR")
	if fwd == "" {
		if req.RemoteAddr == "" {
			// No X-FORWARDED-FOR header and no RemoteAddr set so we want to return an empty string
			// Might as well just use the empty string set in req.RemoteAddr
			return req.RemoteAddr
		}
		// We don't care what port the request came on and typically the go http server will include
		// this in the format of 192.168.1.1:13435 so we split and return just the IP
		// If not port is present the split will fail to match and it is safe to still access the 0th
		// element per the Split docs
		return strings.Split(req.RemoteAddr, ":")[0]
	}
	// If RemoteAddr is empty just return the header
	if req.RemoteAddr == "" {
		return fwd
	}
	// If both a header and RemoteAddr are present return them both, stripping off any port info from the RemoteAddr
	return fmt.Sprintf("%v, %v", fwd, strings.Split(req.RemoteAddr, ":")[0])
}

// NewSourceContext creates a new Context from the existing one with the source added
func NewSourceContext(ctx context.Context, source string) context.Context {
	return context.WithValue(ctx, sourceKey, source)
}

// GetSourceFromCtx extracts the source field from the context
func GetSourceFromCtx(ctx context.Context) string {
	fwd, ok := ctx.Value(sourceKey).(string)
	if !ok {
		return ""
	}
	return fwd
}
