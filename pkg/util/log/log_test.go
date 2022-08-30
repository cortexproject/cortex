package log

import (
	"context"
	"net/http"
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
	request := httpgrpc.HTTPRequest{Headers: []*httpgrpc.Header{&header}}

	ctx := context.Background()
	ctx = ExtractHeadersFromHTTPRequest(ctx, &request)

	headerMap := HeaderMapFromContext(ctx)

	require.Contains(t, headerMap, "TestHeader1")
	require.Contains(t, headerMap, "TestHeader2")
	require.Equal(t, "SomeInformation", headerMap["TestHeader1"])
	require.Equal(t, "ContentsOfTestHeader2", headerMap["TestHeader2"])
}

func TestHeaderMapFromRequestHeaderWithImproperLength(t *testing.T) {
	headerSlice := []string{"TestHeader1", "SomeInformation", "TestHeader2", "ContentsOfTestHeader2", "Test3"}
	header := httpgrpc.Header{Key: HeaderPropagationStringForRequestLogging, Values: headerSlice}
	request := httpgrpc.HTTPRequest{Headers: []*httpgrpc.Header{&header}}
	
	ctx := context.Background()
	ctx = ExtractHeadersFromHTTPRequest(ctx, &request)

	headerMap := HeaderMapFromContext(ctx)

	require.Nil(t, headerMap)

}

func TestInjectHeadersIntoHTTPRequest(t *testing.T) {
	contentsMap := make(map[string]string)
	contentsMap["TestHeader1"] = "RequestID"
	contentsMap["TestHeader2"] = "ContentsOfTestHeader2"

	h := http.Header{}
	req := &http.Request{
		Method:     "GET",
		RequestURI: "/HTTPHeaderTest",
		Body:       http.NoBody,
		Header:     h,
	}
	InjectHeadersIntoHTTPRequest(contentsMap, req)
	headers := req.Header.Values(HeaderPropagationStringForRequestLogging)

	require.NotNil(t, headers)
	require.Equal(t, 4, len(headers))

	for headerName, headerContents := range contentsMap {
		require.Contains(t, headers, headerName)
		require.Contains(t, headers, headerContents)
	}

}

func TestNoHeadersForExtractHeadersFromHTTPRequest(t *testing.T) {
	ctx := context.Background()
	request := httpgrpc.HTTPRequest{Headers: nil}

	ctx = ExtractHeadersFromHTTPRequest(ctx, &request)

	require.Nil(t, HeaderMapFromContext(ctx))

}

func TestDifferentHeaderLengthsForExtractHeadersFromHTTPRequest(t *testing.T) {
	headerSlice := []string{"TestHeader1", "SomeInformation", "TestHeader2", "ContentsOfTestHeader2", "Test3"}
	header := httpgrpc.Header{Key: HeaderPropagationStringForRequestLogging, Values: headerSlice}
	headers := []*httpgrpc.Header{&header}

	ctx := context.Background()
	request := httpgrpc.HTTPRequest{Headers: headers}

	ctx = ExtractHeadersFromHTTPRequest(ctx, &request)

	require.Nil(t, HeaderMapFromContext(ctx))

}

func TestValidInputForExtractHeadersFromHTTPRequest(t *testing.T) {
	headerSlice := []string{"TestHeader1", "SomeInformation", "TestHeader2", "ContentsOfTestHeader2"}
	header := httpgrpc.Header{Key: HeaderPropagationStringForRequestLogging, Values: headerSlice}
	headers := []*httpgrpc.Header{&header}

	ctx := context.Background()
	request := httpgrpc.HTTPRequest{Headers: headers}

	ctx = ExtractHeadersFromHTTPRequest(ctx, &request)

	headerMap := HeaderMapFromContext(ctx)
	require.NotNil(t, headerMap)
	require.Equal(t, 2, len(headerMap))
	for headerName, headerContents := range headerMap {
		require.Contains(t, headerSlice, headerName)
		require.Contains(t, headerSlice, headerContents)
	}

}
