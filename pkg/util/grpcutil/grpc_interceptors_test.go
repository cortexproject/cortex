package grpcutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	util_log "github.com/cortexproject/cortex/pkg/util/log"
)

func TestHTTPHeaderPropagationClientInterceptor(t *testing.T) {
	ctx := context.Background()

	contentsMap := make(map[string]string)
	contentsMap["TestHeader1"] = "RequestID"
	contentsMap["TestHeader2"] = "ContentsOfTestHeader2"
	contentsMap["Test3"] = "SomeInformation"
	ctx = context.WithValue(ctx, util_log.HeaderMapContextKey, contentsMap)

	ctx = putForwardedHeadersIntoMetadata(ctx)

	meta, worked := metadata.FromOutgoingContext(ctx)
	require.True(t, worked)

	headers := meta["httpheaderforwardingnames"]
	assert.Equal(t, 3, len(headers))
	assert.Contains(t, headers, "TestHeader1")
	assert.Contains(t, headers, "TestHeader2")
	assert.Contains(t, headers, "Test3")

	headerContents := meta["httpheaderforwardingcontents"]
	assert.Equal(t, 3, len(headerContents))
	assert.Contains(t, headerContents, "RequestID")
	assert.Contains(t, headerContents, "ContentsOfTestHeader2")
	assert.Contains(t, headerContents, "SomeInformation")
}

func TestExistingValuesInMetadataForHTTPPropagationClientInterceptor(t *testing.T) {
	ctx := context.Background()
	ctx = metadata.AppendToOutgoingContext(ctx, "httpheaderforwardingnames", "testabc123")

	contentsMap := make(map[string]string)
	contentsMap["TestHeader1"] = "RequestID"
	contentsMap["TestHeader2"] = "ContentsOfTestHeader2"
	contentsMap["Test3"] = "SomeInformation"
	ctx = context.WithValue(ctx, util_log.HeaderMapContextKey, contentsMap)

	ctx = putForwardedHeadersIntoMetadata(ctx)

	meta, worked := metadata.FromOutgoingContext(ctx)
	require.True(t, worked)

	contents := meta["httpheaderforwardingnames"]
	assert.Contains(t, contents, "testabc123")
	assert.Equal(t, 1, len(contents))
}

func TestGRPCHeaderInjectionForHTTPPropagationServerInterceptor(t *testing.T) {
	ctx := context.Background()
	testMap := make(map[string]string)
	testMap["httpheaderforwardingnames"] = "Test123"
	testMap["httpheaderforwardingcontents"] = "Results"
	meta := metadata.New(testMap)
	ctx = metadata.NewOutgoingContext(ctx, meta)

	ctx = metadata.AppendToOutgoingContext(ctx, "httpheaderforwardingnames", "TestHeader2")
	ctx = metadata.AppendToOutgoingContext(ctx, "httpheaderforwardingcontents", "Results2")
	md, worked := metadata.FromOutgoingContext(ctx)
	ctx = forwardHeadersFromMetadataHelper(ctx, md, worked)

	headersMap, worked := ctx.Value(util_log.HeaderMapContextKey).(map[string]string)

	require.True(t, worked)
	require.NotNil(t, headersMap)
	assert.Equal(t, 2, len(headersMap))

	assert.Equal(t, headersMap["Test123"], "Results")
	assert.Equal(t, headersMap["TestHeader2"], "Results2")

}

func TestGRPCHeaderDifferentLengthsForHTTPPropagationServerInterceptor(t *testing.T) {
	ctx := context.Background()
	ctx = metadata.AppendToOutgoingContext(ctx, "httpheaderforwardingnames", "Test123")
	ctx = metadata.AppendToOutgoingContext(ctx, "httpheaderforwardingcontents", "Results")
	ctx = metadata.AppendToOutgoingContext(ctx, "httpheaderforwardingcontents", "Results2")

	ctx = pullForwardedHeadersFromMetadata(ctx)

	assert.Nil(t, ctx.Value(util_log.HeaderMapContextKey))
}
