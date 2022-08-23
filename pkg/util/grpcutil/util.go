package grpcutil

import (
	"context"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/gogo/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// IsGRPCContextCanceled returns whether the input error is a GRPC error wrapping
// the context.Canceled error.
func IsGRPCContextCanceled(err error) bool {
	s, ok := status.FromError(err)
	if !ok {
		return false
	}

	return s.Code() == codes.Canceled
}

// HTTPHeaderPropagationServerInterceptor allows for propagation of HTTP Request headers across gRPC calls - works
// alongside HTTPHeaderPropagationClientInterceptor
func HTTPHeaderPropagationServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (resp interface{}, err error) {
		ctx = pullForwardedHeadersFromMetadata(ctx)
		h, err := handler(ctx, req)
		return h, err
	}
}

// pullForwardedHeadersFromMetadata implements HTTPHeaderPropagationServerInterceptor by placing forwarded
// headers into incoming context
func pullForwardedHeadersFromMetadata(ctx context.Context) context.Context {
	meta, worked := metadata.FromIncomingContext(ctx)
	return forwardHeadersFromMetadataHelper(ctx, meta, worked)
}

// forwardHeadersFromMetadataHelper implements pullForwardedHeadersFromMetadata
func forwardHeadersFromMetadataHelper(ctx context.Context, meta metadata.MD, worked bool) context.Context {
	headerMap := make(map[string]string)
	if worked {
		headersSlice := meta["httpheaderforwardingnames"]
		headerContentsSlice := meta["httpheaderforwardingcontents"]
		if len(headersSlice) == len(headerContentsSlice) {
			for i, header := range headersSlice {
				headerMap[header] = headerContentsSlice[i]
			}
			ctx = context.WithValue(ctx, util_log.HeaderMapContextKey, headerMap)
		}
	}
	return ctx
}

// HTTPHeaderPropagationClientInterceptor allows for propagation of HTTP Request headers across gRPC calls - works
// alongside HTTPHeaderPropagationServerInterceptor
func HTTPHeaderPropagationClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = putForwardedHeadersIntoMetadata(ctx)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// putForwardedHeadersIntoMetadata implements HTTPHeaderPropagationClientInterceptor by inserting headers
// that are supposed to be forwarded into metadata of the request
func putForwardedHeadersIntoMetadata(ctx context.Context) context.Context {
	meta, worked := metadata.FromOutgoingContext(ctx)
	if worked {
		if len(meta["httpheaderforwardingnames"]) != 0 || len(meta["httpheaderforwardingcontents"]) != 0 {
			return ctx
		}
	}

	headerContentsMap, ok := ctx.Value(util_log.HeaderMapContextKey).(map[string]string)
	if ok {
		for header, contents := range headerContentsMap {
			ctx = metadata.AppendToOutgoingContext(ctx, "httpheaderforwardingnames", header)
			ctx = metadata.AppendToOutgoingContext(ctx, "httpheaderforwardingcontents", contents)
		}
	}
	return ctx
}
