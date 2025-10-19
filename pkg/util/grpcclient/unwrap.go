package grpcclient

import (
	"context"
	"errors"

	"google.golang.org/grpc"
)

// unwrapErrorStreamClientInterceptor unwraps errors wrapped by OpenTracingStreamClientInterceptor
func unwrapErrorStreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}

		return &unwrapErrorClientStream{
			ClientStream: stream,
		}, nil
	}
}

type unwrapErrorClientStream struct {
	grpc.ClientStream
}

func (s *unwrapErrorClientStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	if err != nil {
		// Try to unwrap the error to get the original error
		if wrappedErr := errors.Unwrap(err); wrappedErr != nil {
			return wrappedErr
		}
	}
	return err
}
