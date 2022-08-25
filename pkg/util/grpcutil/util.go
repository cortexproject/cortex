package grpcutil

import (
	"context"

	"github.com/gogo/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	util_log "github.com/cortexproject/cortex/pkg/util/log"
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
	if worked {
		return util_log.HeaderMapFromMetadata(ctx, meta)
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
		if len(meta[util_log.HeaderPropagationStringForRequestLogging]) != 0 {
			return ctx
		}
	}
	headerContentsMap := util_log.HeaderMapFromContext(ctx)
	if headerContentsMap != nil {
		ctx = util_log.ContextWithMetadataHeaderMap(ctx, headerContentsMap)
	}
	return ctx
}
