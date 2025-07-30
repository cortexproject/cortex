package requestmeta

import (
	"context"
	"net/http"
	"net/textproto"

	"google.golang.org/grpc/metadata"
)

type contextKey int

const (
	requestMetadataContextKey           contextKey = 0
	PropagationStringForRequestMetadata string     = "x-request-metadata-propagation-string"
	// HeaderPropagationStringForRequestLogging is used for backwards compatibility
	HeaderPropagationStringForRequestLogging string = "x-http-header-forwarding-logging"
)

func ContextWithRequestMetadataMap(ctx context.Context, requestContextMap map[string]string) context.Context {
	return context.WithValue(ctx, requestMetadataContextKey, requestContextMap)
}

func MapFromContext(ctx context.Context) map[string]string {
	requestContextMap, ok := ctx.Value(requestMetadataContextKey).(map[string]string)
	if !ok {
		return nil
	}
	return requestContextMap
}

// ContextWithRequestMetadataMapFromHeaders adds MetadataContext headers to context and Removes non-existent headers.
// targetHeaders is passed for backwards compatibility, otherwise header keys should be in header itself.
func ContextWithRequestMetadataMapFromHeaders(ctx context.Context, headers map[string]string, targetHeaders []string) context.Context {
	headerMap := make(map[string]string)
	loggingHeaders := headers[textproto.CanonicalMIMEHeaderKey(LoggingHeadersKey)]
	headerKeys := targetHeaders
	if loggingHeaders != "" {
		headerKeys = LoggingHeaderKeysFromString(loggingHeaders)
		headerKeys = append(headerKeys, LoggingHeadersKey)
	}
	headerKeys = append(headerKeys, RequestIdKey)
	for _, header := range headerKeys {
		if v, ok := headers[textproto.CanonicalMIMEHeaderKey(header)]; ok {
			headerMap[header] = v
		}
	}
	return ContextWithRequestMetadataMap(ctx, headerMap)
}

func InjectMetadataIntoHTTPRequestHeaders(requestMetadataMap map[string]string, request *http.Request) {
	for key, contents := range requestMetadataMap {
		request.Header.Add(key, contents)
	}
}

func ContextWithRequestMetadataMapFromMetadata(ctx context.Context, md metadata.MD) context.Context {
	headersSlice, ok := md[PropagationStringForRequestMetadata]

	// we want to check old key if no data
	if !ok {
		headersSlice, ok = md[HeaderPropagationStringForRequestLogging]
	}

	if !ok || len(headersSlice)%2 == 1 {
		return ctx
	}

	requestMetadataMap := make(map[string]string)
	for i := 0; i < len(headersSlice); i += 2 {
		requestMetadataMap[headersSlice[i]] = headersSlice[i+1]
	}

	return ContextWithRequestMetadataMap(ctx, requestMetadataMap)
}
