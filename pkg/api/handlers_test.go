package api

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexHandlerPrefix(t *testing.T) {
	for _, tc := range []struct {
		prefix    string
		toBeFound string
	}{
		{prefix: "", toBeFound: "<a href=\"/ingester/ring\">"},
		{prefix: "/test", toBeFound: "<a href=\"/test/ingester/ring\">"},
		// All the extra slashed are cleaned up in the result.
		{prefix: "///test///", toBeFound: "<a href=\"/test/ingester/ring\">"},
	} {
		h := indexHandler(tc.prefix)

		req := httptest.NewRequest("GET", "/", nil)
		resp := httptest.NewRecorder()

		h.ServeHTTP(resp, req)

		require.Equal(t, 200, resp.Code)
		require.True(t, strings.Contains(resp.Body.String(), tc.toBeFound))
	}
}
