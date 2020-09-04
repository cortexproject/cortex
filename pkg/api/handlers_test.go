package api

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexHandlerPrefix(t *testing.T) {
	c := newIndexPageContent()
	c.AddLink(SectionAdminEndpoints, "/ingester/ring", "Ingester Ring")

	for _, tc := range []struct {
		prefix    string
		toBeFound string
	}{
		{prefix: "", toBeFound: "<a href=\"/ingester/ring\">"},
		{prefix: "/test", toBeFound: "<a href=\"/test/ingester/ring\">"},
		// All the extra slashed are cleaned up in the result.
		{prefix: "///test///", toBeFound: "<a href=\"/test/ingester/ring\">"},
	} {
		h := indexHandler(tc.prefix, c)

		req := httptest.NewRequest("GET", "/", nil)
		resp := httptest.NewRecorder()

		h.ServeHTTP(resp, req)

		require.Equal(t, 200, resp.Code)
		require.True(t, strings.Contains(resp.Body.String(), tc.toBeFound))
	}
}

func TestIndexPageContent(t *testing.T) {
	c := newIndexPageContent()
	c.AddLink(SectionAdminEndpoints, "/ingester/ring", "Ingester Ring")
	c.AddLink(SectionAdminEndpoints, "/store-gateway/ring", "Store Gateway Ring")
	c.AddLink(SectionDangerous, "/shutdown", "Shutdown")

	h := indexHandler("", c)

	req := httptest.NewRequest("GET", "/", nil)
	resp := httptest.NewRecorder()

	h.ServeHTTP(resp, req)

	require.Equal(t, 200, resp.Code)
	require.True(t, strings.Contains(resp.Body.String(), SectionAdminEndpoints))
	require.True(t, strings.Contains(resp.Body.String(), SectionDangerous))
	require.True(t, strings.Contains(resp.Body.String(), "Store Gateway Ring"))
	require.True(t, strings.Contains(resp.Body.String(), "/shutdown"))
	require.False(t, strings.Contains(resp.Body.String(), "/compactor/ring"))
}
