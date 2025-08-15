package requestmeta

import "context"

const RequestSourceKey = "x-cortex-request-source"

const (
	SourceAPI   = "api"
	SourceRuler = "ruler"
)

func ContextWithRequestSource(ctx context.Context, source string) context.Context {
	metadataMap := MapFromContext(ctx)
	if metadataMap == nil {
		metadataMap = make(map[string]string)
	}
	metadataMap[RequestSourceKey] = source
	return ContextWithRequestMetadataMap(ctx, metadataMap)
}

func RequestFromRuler(ctx context.Context) bool {
	metadataMap := MapFromContext(ctx)
	if metadataMap == nil {
		return false
	}
	return metadataMap[RequestSourceKey] == SourceRuler
}
