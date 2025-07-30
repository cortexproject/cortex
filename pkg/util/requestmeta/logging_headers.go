package requestmeta

import (
	"context"
	"strings"
)

const (
	LoggingHeadersKey       = "x-request-logging-headers-key"
	loggingHeadersDelimiter = ","
)

func LoggingHeaderKeysToString(targetHeaders []string) string {
	return strings.Join(targetHeaders, loggingHeadersDelimiter)
}

func LoggingHeaderKeysFromString(headerKeysString string) []string {
	return strings.Split(headerKeysString, loggingHeadersDelimiter)
}

func LoggingHeadersFromContext(ctx context.Context) map[string]string {
	metadataMap := MapFromContext(ctx)
	if metadataMap == nil {
		return nil
	}
	loggingHeadersString := metadataMap[LoggingHeadersKey]
	if loggingHeadersString == "" {
		// Backward compatibility: if no specific headers are listed, return all metadata
		result := make(map[string]string, len(metadataMap))
		for k, v := range metadataMap {
			result[k] = v
		}
		return result
	}

	result := make(map[string]string)
	for _, header := range LoggingHeaderKeysFromString(loggingHeadersString) {
		if v, ok := metadataMap[header]; ok {
			result[header] = v
		}
	}
	return result
}

func LoggingHeadersAndRequestIdFromContext(ctx context.Context) map[string]string {
	metadataMap := MapFromContext(ctx)
	if metadataMap == nil {
		return nil
	}

	loggingHeaders := LoggingHeadersFromContext(ctx)
	reqId := RequestIdFromContext(ctx)
	loggingHeaders[RequestIdKey] = reqId

	return loggingHeaders
}
