package requestmeta

import (
	"context"
	"net/http"
	"net/textproto"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestRequestMetadataMapFromMetadata(t *testing.T) {
	md := metadata.New(nil)
	md.Append(PropagationStringForRequestMetadata, "TestHeader1", "SomeInformation", "TestHeader2", "ContentsOfTestHeader2")

	ctx := context.Background()

	ctx = ContextWithRequestMetadataMapFromMetadata(ctx, md)

	requestMetadataMap := MapFromContext(ctx)

	require.Contains(t, requestMetadataMap, "TestHeader1")
	require.Contains(t, requestMetadataMap, "TestHeader2")
	require.Equal(t, "SomeInformation", requestMetadataMap["TestHeader1"])
	require.Equal(t, "ContentsOfTestHeader2", requestMetadataMap["TestHeader2"])
}

func TestRequestMetadataMapFromMetadataWithImproperLength(t *testing.T) {
	md := metadata.New(nil)
	md.Append(PropagationStringForRequestMetadata, "TestHeader1", "SomeInformation", "TestHeader2", "ContentsOfTestHeader2", "Test3")

	ctx := context.Background()

	ctx = ContextWithRequestMetadataMapFromMetadata(ctx, md)

	requestMetadataMap := MapFromContext(ctx)
	require.Nil(t, requestMetadataMap)
}

func TestContextWithRequestMetadataMapFromHeaders_WithLoggingHeaders(t *testing.T) {
	headers := map[string]string{
		textproto.CanonicalMIMEHeaderKey("X-Request-ID"):    "1234",
		textproto.CanonicalMIMEHeaderKey("X-User-ID"):       "user5678",
		textproto.CanonicalMIMEHeaderKey(LoggingHeadersKey): "X-Request-ID,X-User-ID",
	}

	ctx := context.Background()
	ctx = ContextWithRequestMetadataMapFromHeaders(ctx, headers, nil)

	requestMetadataMap := MapFromContext(ctx)

	require.Contains(t, requestMetadataMap, "X-Request-ID")
	require.Contains(t, requestMetadataMap, "X-User-ID")
	require.Equal(t, "1234", requestMetadataMap["X-Request-ID"])
	require.Equal(t, "user5678", requestMetadataMap["X-User-ID"])
}

func TestContextWithRequestMetadataMapFromHeaders_BackwardCompatibleTargetHeaders(t *testing.T) {
	headers := map[string]string{
		textproto.CanonicalMIMEHeaderKey("X-Legacy-Header"): "legacy-value",
	}

	ctx := context.Background()
	ctx = ContextWithRequestMetadataMapFromHeaders(ctx, headers, []string{"X-Legacy-Header"})

	requestMetadataMap := MapFromContext(ctx)

	require.Contains(t, requestMetadataMap, "X-Legacy-Header")
	require.Equal(t, "legacy-value", requestMetadataMap["X-Legacy-Header"])
}

func TestContextWithRequestMetadataMapFromHeaders_OnlyMatchingKeysUsed(t *testing.T) {
	headers := map[string]string{
		textproto.CanonicalMIMEHeaderKey("X-Some-Header"):   "value1",
		textproto.CanonicalMIMEHeaderKey("Unused-Header"):   "value2",
		textproto.CanonicalMIMEHeaderKey(LoggingHeadersKey): "X-Some-Header",
	}

	ctx := context.Background()
	ctx = ContextWithRequestMetadataMapFromHeaders(ctx, headers, nil)

	requestMetadataMap := MapFromContext(ctx)

	require.Equal(t, "value1", requestMetadataMap["X-Some-Header"])
}

func TestInjectMetadataIntoHTTPRequestHeaders(t *testing.T) {
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
	InjectMetadataIntoHTTPRequestHeaders(contentsMap, req)

	header1 := req.Header.Values("TestHeader1")
	header2 := req.Header.Values("TestHeader2")

	require.NotNil(t, header1)
	require.NotNil(t, header2)
	require.Equal(t, 1, len(header1))
	require.Equal(t, 1, len(header2))

	require.Equal(t, "RequestID", header1[0])
	require.Equal(t, "ContentsOfTestHeader2", header2[0])

}
