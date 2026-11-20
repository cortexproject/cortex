package clientid

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
)

func TestExtractFromHTTPRequest(t *testing.T) {
	r := &http.Request{Header: http.Header{}}
	assert.Equal(t, "", ExtractFromHTTPRequest(r), "absent header is not an error")

	r.Header.Set(HeaderName, "team-a")
	assert.Equal(t, "team-a", ExtractFromHTTPRequest(r))
}

func TestExtractFromGRPCContext(t *testing.T) {
	assert.Equal(t, "", ExtractFromGRPCContext(context.Background()))

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-scope-clientid", "team-a"))
	assert.Equal(t, "team-a", ExtractFromGRPCContext(ctx))
}

func TestContextRoundTrip(t *testing.T) {
	assert.Equal(t, "", FromContext(context.Background()))

	ctx := InjectIntoContext(context.Background(), "team-a")
	assert.Equal(t, "team-a", FromContext(ctx))

	// An empty identity leaves the context untouched.
	assert.Equal(t, "", FromContext(InjectIntoContext(context.Background(), "")))
}
