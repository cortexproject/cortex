package grpcclient

import (
	"context"
	"errors"
	"testing"

	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type mockClientStream struct {
	recvErr error
}

func (m *mockClientStream) RecvMsg(msg interface{}) error {
	return m.recvErr
}

func (m *mockClientStream) Header() (metadata.MD, error) {
	return nil, nil
}

func (m *mockClientStream) Trailer() metadata.MD {
	return nil
}

func (m *mockClientStream) CloseSend() error {
	return nil
}

func (m *mockClientStream) Context() context.Context {
	return context.Background()
}

func (m *mockClientStream) SendMsg(interface{}) error {
	return nil
}

func TestUnwrapErrorStreamClientInterceptor(t *testing.T) {
	// Create a mock tracer
	tracer := mocktracer.New()
	opentracing.SetGlobalTracer(tracer)

	originalErr := errors.New("original error")
	// Create a mock stream that returns the original error
	mockStream := &mockClientStream{
		recvErr: originalErr,
	}

	// Create a mock streamer that returns our mock stream
	mockStreamer := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return mockStream, nil
	}

	// Create the interceptor chain
	otStreamInterceptor := otgrpc.OpenTracingStreamClientInterceptor(tracer)
	interceptors := []grpc.StreamClientInterceptor{
		unwrapErrorStreamClientInterceptor(),
		otStreamInterceptor,
	}

	// Chain the interceptors
	chainedStreamer := mockStreamer
	for i := len(interceptors) - 1; i >= 0; i-- {
		chainedStreamer = func(interceptor grpc.StreamClientInterceptor, next grpc.Streamer) grpc.Streamer {
			return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				return interceptor(ctx, desc, cc, method, next, opts...)
			}
		}(interceptors[i], chainedStreamer)
	}

	// Call the chained streamer
	ctx := context.Background()
	stream, err := chainedStreamer(ctx, &grpc.StreamDesc{}, nil, "test")
	require.NoError(t, err)
	var msg interface{}
	err = stream.RecvMsg(&msg)
	require.Error(t, err)
	require.EqualError(t, err, originalErr.Error())

	// Only wrap OpenTracingStreamClientInterceptor.
	chainedStreamerWithoutUnwrapErr := func(interceptor grpc.StreamClientInterceptor, next grpc.Streamer) grpc.Streamer {
		return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			return interceptor(ctx, desc, cc, method, next, opts...)
		}
	}(otStreamInterceptor, mockStreamer)
	stream, err = chainedStreamerWithoutUnwrapErr(ctx, &grpc.StreamDesc{}, nil, "test")
	require.NoError(t, err)
	err = stream.RecvMsg(&msg)
	require.Error(t, err)
	// Error is wrapped by OpenTracingStreamClientInterceptor and not unwrapped.
	require.Contains(t, err.Error(), "failed to receive message")
}
