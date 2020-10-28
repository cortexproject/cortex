package middleware

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/weaveworks/common/user"
)

type withPropagator struct {
	propagator user.Propagator
}

func WithPropagator(p user.Propagator) *withPropagator {
	return &withPropagator{propagator: p}
}

// ClientUserHeaderInterceptor propagates the user ID from the context to gRPC metadata, which eventually ends up as a HTTP2 header.
func (w *withPropagator) ClientUserHeaderInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	ctx, err := w.propagator.InjectIntoGRPCRequest(ctx)
	if err != nil {
		return err
	}

	return invoker(ctx, method, req, reply, cc, opts...)
}

func ClientUserHeaderInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	return WithPropagator(user.NewOrgIDPropagator()).ClientUserHeaderInterceptor(ctx, method, req, reply, cc, invoker, opts...)
}

// StreamClientUserHeaderInterceptor propagates the user ID from the context to gRPC metadata, which eventually ends up as a HTTP2 header.
// For streaming gRPC requests.
func (w *withPropagator) StreamClientUserHeaderInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	ctx, err := w.propagator.InjectIntoGRPCRequest(ctx)
	if err != nil {
		return nil, err
	}

	return streamer(ctx, desc, cc, method, opts...)
}

func StreamClientUserHeaderInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return WithPropagator(user.NewOrgIDPropagator()).StreamClientUserHeaderInterceptor(ctx, desc, cc, method, streamer, opts...)
}

// ServerUserHeaderInterceptor propagates the user ID from the gRPC metadata back to our context.
func (w *withPropagator) ServerUserHeaderInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	ctx, err := w.propagator.ExtractFromGRPCRequest(ctx)
	if err != nil {
		return nil, err
	}

	return handler(ctx, req)
}

func ServerUserHeaderInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return WithPropagator(user.NewOrgIDPropagator()).ServerUserHeaderInterceptor(ctx, req, info, handler)
}

// StreamServerUserHeaderInterceptor propagates the user ID from the gRPC metadata back to our context.
func (w *withPropagator) StreamServerUserHeaderInterceptor(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx, err := w.propagator.ExtractFromGRPCRequest(ss.Context())
	if err != nil {
		return err
	}

	return handler(srv, serverStream{
		ctx:          ctx,
		ServerStream: ss,
	})
}

func StreamServerUserHeaderInterceptor(srv interface{}, ss grpc.ServerStream, ssi *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return WithPropagator(user.NewOrgIDPropagator()).StreamServerUserHeaderInterceptor(srv, ss, ssi, handler)
}

type serverStream struct {
	ctx context.Context
	grpc.ServerStream
}

func (ss serverStream) Context() context.Context {
	return ss.ctx
}
