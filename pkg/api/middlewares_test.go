package api

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util/requestmeta"
)

func TestHeaderInjection(t *testing.T) {
	middleware := HTTPHeaderMiddleware{TargetHeaders: []string{"TestHeader1", "TestHeader2", "Test3"}}
	ctx := context.Background()
	h := http.Header{}
	contentsMap := make(map[string]string)
	contentsMap["TestHeader1"] = "RequestID"
	contentsMap["TestHeader2"] = "ContentsOfTestHeader2"
	contentsMap["Test3"] = "SomeInformation"

	h.Add("TestHeader1", contentsMap["TestHeader1"])
	h.Add("TestHeader2", contentsMap["TestHeader2"])
	h.Add("Test3", contentsMap["Test3"])

	req := &http.Request{
		Method:     "GET",
		RequestURI: "/HTTPHeaderTest",
		Body:       http.NoBody,
		Header:     h,
	}

	req = req.WithContext(ctx)
	req = middleware.injectRequestContext(req)

	headerMap := requestmeta.MapFromContext(req.Context())
	require.NotNil(t, headerMap)

	for _, header := range middleware.TargetHeaders {
		require.Equal(t, contentsMap[header], headerMap[header])
	}
	for header, contents := range contentsMap {
		require.Equal(t, contents, headerMap[header])
	}
}

func TestExistingHeaderInContextIsNotOverridden(t *testing.T) {
	middleware := HTTPHeaderMiddleware{TargetHeaders: []string{"TestHeader1", "TestHeader2", "Test3"}}
	ctx := context.Background()

	h := http.Header{}
	contentsMap := make(map[string]string)
	contentsMap["TestHeader1"] = "RequestID"
	contentsMap["TestHeader2"] = "ContentsOfTestHeader2"
	contentsMap["Test3"] = "SomeInformation"

	h.Add("TestHeader1", "Fail1")
	h.Add("TestHeader2", "Fail2")
	h.Add("Test3", "Fail3")

	ctx = requestmeta.ContextWithRequestMetadataMap(ctx, contentsMap)
	req := &http.Request{
		Method:     "GET",
		RequestURI: "/HTTPHeaderTest",
		Body:       http.NoBody,
		Header:     h,
	}

	req = req.WithContext(ctx)
	req = middleware.injectRequestContext(req)

	require.Equal(t, contentsMap, requestmeta.MapFromContext(req.Context()))

}

func TestRequestIdInjection(t *testing.T) {
	middleware := HTTPHeaderMiddleware{
		RequestIdHeader: "X-Request-ID",
	}

	req := &http.Request{
		Method:     "GET",
		RequestURI: "/test",
		Body:       http.NoBody,
		Header:     http.Header{},
	}
	req = req.WithContext(context.Background())
	req = middleware.injectRequestContext(req)

	requestID := requestmeta.RequestIdFromContext(req.Context())
	require.NotEmpty(t, requestID, "Request ID should be generated if not provided")
}

func TestRequestIdFromHeaderIsUsed(t *testing.T) {
	const providedID = "my-test-id-123"

	middleware := HTTPHeaderMiddleware{
		RequestIdHeader: "X-Request-ID",
	}

	h := http.Header{}
	h.Add("X-Request-ID", providedID)

	req := &http.Request{
		Method:     "GET",
		RequestURI: "/test",
		Body:       http.NoBody,
		Header:     h,
	}
	req = req.WithContext(context.Background())
	req = middleware.injectRequestContext(req)

	requestID := requestmeta.RequestIdFromContext(req.Context())
	require.Equal(t, providedID, requestID, "Request ID from header should be used")
}

func TestTargetHeaderAndRequestIdHeaderOverlap(t *testing.T) {
	const headerKey = "X-Request-ID"
	const providedID = "overlap-id-456"

	middleware := HTTPHeaderMiddleware{
		TargetHeaders:   []string{headerKey, "Other-Header"},
		RequestIdHeader: headerKey,
	}

	h := http.Header{}
	h.Add(headerKey, providedID)
	h.Add("Other-Header", "some-value")

	req := &http.Request{
		Method:     "GET",
		RequestURI: "/test",
		Body:       http.NoBody,
		Header:     h,
	}
	req = req.WithContext(context.Background())
	req = middleware.injectRequestContext(req)

	ctxMap := requestmeta.MapFromContext(req.Context())
	requestID := requestmeta.RequestIdFromContext(req.Context())
	require.Equal(t, providedID, ctxMap[headerKey], "Header value should be correctly stored")
	require.Equal(t, providedID, requestID, "Request ID should come from the overlapping header")
}
