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
	ctx = util_log.ContextWithHeaderMap(ctx, contentsMap)

	ctx = injectForwardedHeadersIntoMetadata(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)

	headers := md[util_log.HeaderPropagationStringForRequestLogging]
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
	ctx = metadata.AppendToOutgoingContext(ctx, util_log.HeaderPropagationStringForRequestLogging, "testabc123")

	contentsMap := make(map[string]string)
	contentsMap["TestHeader1"] = "RequestID"
	contentsMap["TestHeader2"] = "ContentsOfTestHeader2"
	contentsMap["Test3"] = "SomeInformation"
	ctx = util_log.ContextWithHeaderMap(ctx, contentsMap)

	ctx = injectForwardedHeadersIntoMetadata(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)

	contents := md[util_log.HeaderPropagationStringForRequestLogging]
	assert.Contains(t, contents, "testabc123")
	assert.Equal(t, 1, len(contents))
}

func TestGRPCHeaderInjectionForHTTPPropagationServerInterceptor(t *testing.T) {
	ctx := context.Background()
	testMap := make(map[string]string)

	testMap["Test1"] = "Results"
	testMap["TestHeader2"] = "Results2"

	ctx = metadata.NewOutgoingContext(ctx, nil)
	ctx = util_log.ContextWithHeaderMap(ctx, testMap)
	ctx = injectForwardedHeadersIntoMetadata(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)
	ctx = util_log.ContextWithHeaderMapFromMetadata(ctx, md)

	headersMap := util_log.HeaderMapFromContext(ctx)

	require.NotNil(t, headersMap)
	assert.Equal(t, 2, len(headersMap))

	assert.Equal(t, "Results", headersMap["Test1"])
	assert.Equal(t, "Results2", headersMap["TestHeader2"])

}

func TestGRPCHeaderDifferentLengthsForHTTPPropagationServerInterceptor(t *testing.T) {
	ctx := context.Background()
	ctx = metadata.AppendToOutgoingContext(ctx, util_log.HeaderPropagationStringForRequestLogging, "Test123")
	ctx = metadata.AppendToOutgoingContext(ctx, util_log.HeaderPropagationStringForRequestLogging, "Results")
	ctx = metadata.AppendToOutgoingContext(ctx, util_log.HeaderPropagationStringForRequestLogging, "Results2")

	ctx = extractForwardedHeadersFromMetadata(ctx)

	assert.Nil(t, util_log.HeaderMapFromContext(ctx))
}
