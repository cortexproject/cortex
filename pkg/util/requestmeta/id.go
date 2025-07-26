package requestmeta

import "context"

const RequestIdKey = "x-cortex-request-id"

func RequestIdFromContext(ctx context.Context) string {
	metadataMap := MapFromContext(ctx)
	if metadataMap == nil {
		return ""
	}
	return metadataMap[RequestIdKey]
}

func ContextWithRequestId(ctx context.Context, reqId string) context.Context {
	metadataMap := MapFromContext(ctx)
	if metadataMap == nil {
		metadataMap = make(map[string]string)
	}
	metadataMap[RequestIdKey] = reqId
	return ContextWithRequestMetadataMap(ctx, metadataMap)
}
