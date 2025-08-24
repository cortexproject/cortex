package grpcutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/cortexproject/cortex/pkg/util/requestmeta"
)

func TestHTTPHeaderPropagationClientInterceptor(t *testing.T) {
	ctx := context.Background()

	contentsMap := make(map[string]string)
	contentsMap["TestHeader1"] = "RequestID"
	contentsMap["TestHeader2"] = "ContentsOfTestHeader2"
	contentsMap["Test3"] = "SomeInformation"
	ctx = requestmeta.ContextWithRequestMetadataMap(ctx, contentsMap)

	ctx = injectForwardedRequestMetadata(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)

	headers := md[requestmeta.PropagationStringForRequestMetadata]
	assert.Equal(t, 6, len(headers))
	assert.Contains(t, headers, "TestHeader1")
	assert.Contains(t, headers, "TestHeader2")
	assert.Contains(t, headers, "Test3")
	assert.Contains(t, headers, "RequestID")
	assert.Contains(t, headers, "ContentsOfTestHeader2")
	assert.Contains(t, headers, "SomeInformation")
}

func TestExistingValuesInMetadataForHTTPPropagationClientInterceptor(t *testing.T) {
	ctx := context.Background()
	ctx = metadata.AppendToOutgoingContext(ctx, requestmeta.PropagationStringForRequestMetadata, "testabc123")

	contentsMap := make(map[string]string)
	contentsMap["TestHeader1"] = "RequestID"
	contentsMap["TestHeader2"] = "ContentsOfTestHeader2"
	contentsMap["Test3"] = "SomeInformation"
	ctx = requestmeta.ContextWithRequestMetadataMap(ctx, contentsMap)

	ctx = injectForwardedRequestMetadata(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)

	contents := md[requestmeta.PropagationStringForRequestMetadata]
	assert.Contains(t, contents, "testabc123")
	assert.Equal(t, 1, len(contents))
}

func TestGRPCHeaderInjectionForHTTPPropagationServerInterceptor(t *testing.T) {
	ctx := context.Background()
	testMap := make(map[string]string)

	testMap["Test1"] = "Results"
	testMap["TestHeader2"] = "Results2"

	ctx = metadata.NewOutgoingContext(ctx, nil)
	ctx = requestmeta.ContextWithRequestMetadataMap(ctx, testMap)
	ctx = injectForwardedRequestMetadata(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)
	ctx = requestmeta.ContextWithRequestMetadataMapFromMetadata(ctx, md)

	headersMap := requestmeta.MapFromContext(ctx)

	require.NotNil(t, headersMap)
	assert.Equal(t, 2, len(headersMap))

	assert.Equal(t, "Results", headersMap["Test1"])
	assert.Equal(t, "Results2", headersMap["TestHeader2"])

}

func TestGRPCHeaderDifferentLengthsForHTTPPropagationServerInterceptor(t *testing.T) {
	ctx := context.Background()
	ctx = metadata.AppendToOutgoingContext(ctx, requestmeta.PropagationStringForRequestMetadata, "Test123")
	ctx = metadata.AppendToOutgoingContext(ctx, requestmeta.PropagationStringForRequestMetadata, "Results")
	ctx = metadata.AppendToOutgoingContext(ctx, requestmeta.PropagationStringForRequestMetadata, "Results2")

	ctx = extractForwardedRequestMetadataFromMetadata(ctx)

	assert.Nil(t, requestmeta.MapFromContext(ctx))
}
