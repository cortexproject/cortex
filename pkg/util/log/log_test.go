package log

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"google.golang.org/grpc/metadata"
)

func TestHeaderMapFromMetadata(t *testing.T) {
	md := metadata.New(nil)
	md.Append(HeaderPropagationStringForRequestLogging, "TestHeader1", "SomeInformation", "TestHeader2", "ContentsOfTestHeader2")

	ctx := context.Background()

	ctx = ContextWithHeaderMapFromMetadata(ctx, md)

	headerMap := HeaderMapFromContext(ctx)

	require.Contains(t, headerMap, "TestHeader1")
	require.Contains(t, headerMap, "TestHeader2")
	require.Equal(t, "SomeInformation", headerMap["TestHeader1"])
	require.Equal(t, "ContentsOfTestHeader2", headerMap["TestHeader2"])
}

func TestHeaderMapFromMetadataWithImproperLength(t *testing.T) {
	md := metadata.New(nil)
	md.Append(HeaderPropagationStringForRequestLogging, "TestHeader1", "SomeInformation", "TestHeader2", "ContentsOfTestHeader2", "Test3")

	ctx := context.Background()

	ctx = ContextWithHeaderMapFromMetadata(ctx, md)

	headerMap := HeaderMapFromContext(ctx)
	require.Nil(t, headerMap)
}

func TestHeaderMapFromRequestHeader(t *testing.T) {
	headerSlice := []string{"TestHeader1", "SomeInformation", "TestHeader2", "ContentsOfTestHeader2"}
	header := httpgrpc.Header{Key: HeaderPropagationStringForRequestLogging, Values: headerSlice}
	ctx := context.Background()
	ctx = ContextWithHeaderMapFromRequestHeader(ctx, &header)

	headerMap := HeaderMapFromContext(ctx)

	require.Contains(t, headerMap, "TestHeader1")
	require.Contains(t, headerMap, "TestHeader2")
	require.Equal(t, "SomeInformation", headerMap["TestHeader1"])
	require.Equal(t, "ContentsOfTestHeader2", headerMap["TestHeader2"])
}

func TestHeaderMapFromRequestHeaderWithImproperLength(t *testing.T) {
	headerSlice := []string{"TestHeader1", "SomeInformation", "TestHeader2", "ContentsOfTestHeader2", "Test3"}
	header := httpgrpc.Header{Key: HeaderPropagationStringForRequestLogging, Values: headerSlice}
	ctx := context.Background()
	ctx = ContextWithHeaderMapFromRequestHeader(ctx, &header)

	headerMap := HeaderMapFromContext(ctx)

	require.Nil(t, headerMap)

}
