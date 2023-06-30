package grpcclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/cortexproject/cortex/pkg/cortexpb"
)

func TestUnarySigningHandler(t *testing.T) {
	ctx := context.Background()
	ctx = user.InjectOrgID(ctx, "user-1")
	w := &cortexpb.WriteRequest{}

	// Sign Request
	err := UnarySigningClientInterceptor(ctx, "", w, w, nil, func(c context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		ctx = c
		return nil
	})

	require.NoError(t, err)

	// Verify the outgoing context
	md, ok := metadata.FromOutgoingContext(ctx)
	signature, err := w.Sign(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, md[reqSignHeaderName][0], signature)
	ctx = metadata.NewIncomingContext(ctx, md)

	// Verify signature on the server side
	_, err = UnarySigningServerInterceptor(ctx, w, nil, func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	})
	require.NoError(t, err)

	// Change user id and make sure the request signature mismatch
	ctx = user.InjectOrgID(ctx, "user-2")
	_, err = UnarySigningServerInterceptor(ctx, w, nil, func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	})

	require.ErrorIs(t, err, ErrSignatureMismatch)

	// Return error when signature is not present
	ctx = user.InjectOrgID(context.Background(), "user-")

	_, err = UnarySigningServerInterceptor(ctx, w, nil, func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	})

	require.ErrorIs(t, err, ErrSignatureNotPresent)

	// Return error when multiples signatures are present
	md[reqSignHeaderName] = append(md[reqSignHeaderName], "sig1", "sig2")
	ctx = metadata.NewOutgoingContext(ctx, md)
	err = UnarySigningClientInterceptor(ctx, "", w, w, nil, func(c context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		ctx = c
		return nil
	})
	require.ErrorIs(t, err, ErrMultipleSignaturePresent)
}
