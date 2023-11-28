package httpgrpcutil

import (
	"github.com/weaveworks/common/httpgrpc"
)

// GetHeader is similar to http.Header.Get, which gets the first value associated with the given key.
// If there are no values associated with the key, it returns "".
func GetHeader(r httpgrpc.HTTPRequest, key string) string {
	values := GetHeaderValues(r, key)
	if len(values) == 0 {
		return ""
	}

	return values[0]
}

// GetHeaderValues is similar to http.Header.Values, which returns all values associated with the given key.
func GetHeaderValues(r httpgrpc.HTTPRequest, key string) []string {
	for _, header := range r.Headers {
		if header.GetKey() == key {
			return header.GetValues()
		}
	}

	return []string{}
}
