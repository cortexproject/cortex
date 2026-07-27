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
	err := UnarySigningClientInterceptor(ctx, "", w, w, nil, func(c context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
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
	_, err = UnarySigningServerInterceptor(ctx, w, nil, func(ctx context.Context, req any) (any, error) {
		return nil, nil
	})
	require.NoError(t, err)

	// Change user id and make sure the request signature mismatch
	ctx = user.InjectOrgID(ctx, "user-2")
	_, err = UnarySigningServerInterceptor(ctx, w, nil, func(ctx context.Context, req any) (any, error) {
		return nil, nil
	})

	require.ErrorIs(t, err, ErrSignatureMismatch)

	// Return error when signature is not present
	ctx = user.InjectOrgID(context.Background(), "user-")

	_, err = UnarySigningServerInterceptor(ctx, w, nil, func(ctx context.Context, req any) (any, error) {
		return nil, nil
	})

	require.ErrorIs(t, err, ErrSignatureNotPresent)

	// Return error when multiples signatures are present
	md[reqSignHeaderName] = append(md[reqSignHeaderName], "sig1", "sig2")
	ctx = metadata.NewOutgoingContext(ctx, md)
	err = UnarySigningClientInterceptor(ctx, "", w, w, nil, func(c context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		ctx = c
		return nil
	})
	require.ErrorIs(t, err, ErrMultipleSignaturePresent)
}

// mockServerStream is a minimal grpc.ServerStream stub for testing stream interceptors.
type mockServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockServerStream) Context() context.Context { return m.ctx }

var pushStreamInfo = &grpc.StreamServerInfo{FullMethod: pushStreamFullMethod}

func TestStreamSigningInterceptors(t *testing.T) {
	const testKey = "super-secret-signing-key"
	orgID := "ingester-fake-stream-push-worker-0"

	sign := func(key string) metadata.MD {
		clientCtx := user.InjectOrgID(context.Background(), orgID)
		var capturedCtx context.Context
		interceptor := NewStreamSigningClientInterceptor(key)
		_, _ = interceptor(clientCtx, nil, nil, pushStreamFullMethod,
			func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				capturedCtx = ctx
				return nil, nil
			})
		outMD, _ := metadata.FromOutgoingContext(capturedCtx)
		return outMD
	}

	verify := func(interceptor grpc.StreamServerInterceptor, incomingMD metadata.MD) error {
		serverCtx := metadata.NewIncomingContext(user.InjectOrgID(context.Background(), orgID), incomingMD)
		ss := &mockServerStream{ctx: serverCtx}
		return interceptor(nil, ss, pushStreamInfo, func(srv any, stream grpc.ServerStream) error { return nil })
	}

	t.Run("valid signature is accepted", func(t *testing.T) {
		interceptor := NewStreamSigningServerInterceptor(testKey)
		require.NoError(t, verify(interceptor, sign(testKey)))
	})

	t.Run("wrong key is rejected with ErrSignatureMismatch", func(t *testing.T) {
		interceptor := NewStreamSigningServerInterceptor(testKey)
		err := verify(interceptor, sign("wrong-key"))
		require.ErrorIs(t, err, ErrSignatureMismatch)
	})

	t.Run("missing signature is rejected with ErrSignatureNotPresent", func(t *testing.T) {
		// No client interceptor – server receives a stream with no signature header.
		interceptor := NewStreamSigningServerInterceptor(testKey)
		err := verify(interceptor, metadata.New(nil))
		require.ErrorIs(t, err, ErrSignatureNotPresent)
	})

	t.Run("non-PushStream method bypasses verification", func(t *testing.T) {
		// Server interceptor must pass through unrelated streaming RPCs unchanged.
		interceptor := NewStreamSigningServerInterceptor(testKey)
		serverCtx := metadata.NewIncomingContext(user.InjectOrgID(context.Background(), orgID), metadata.New(nil))
		ss := &mockServerStream{ctx: serverCtx}
		otherInfo := &grpc.StreamServerInfo{FullMethod: "/cortex.Querier/QueryStream"}
		err := interceptor(nil, ss, otherInfo, func(srv any, stream grpc.ServerStream) error { return nil })
		require.NoError(t, err)
	})
}

// TestStreamSigningKeyRotation verifies that NewStreamSigningServerInterceptor accepts a
// signature produced with any of the configured keys, enabling zero-downtime key rotation.
func TestStreamSigningKeyRotation(t *testing.T) {
	const oldKey = "old-secret"
	const newKey = "new-secret"
	orgID := "ingester-addr-stream-push-worker-0"

	sign := func(key string) metadata.MD {
		clientCtx := user.InjectOrgID(context.Background(), orgID)
		var capturedCtx context.Context
		_, _ = NewStreamSigningClientInterceptor(key)(clientCtx, nil, nil, pushStreamFullMethod,
			func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				capturedCtx = ctx
				return nil, nil
			})
		outMD, _ := metadata.FromOutgoingContext(capturedCtx)
		return outMD
	}

	verify := func(interceptor grpc.StreamServerInterceptor, incomingMD metadata.MD) error {
		serverCtx := metadata.NewIncomingContext(user.InjectOrgID(context.Background(), orgID), incomingMD)
		ss := &mockServerStream{ctx: serverCtx}
		return interceptor(nil, ss, pushStreamInfo, func(srv any, stream grpc.ServerStream) error { return nil })
	}

	// Step1: Ingester is configured with both keys (transition period).
	// Distributor still signs with oldKey (not yet redeployed).
	transitInterceptor := NewStreamSigningServerInterceptor(newKey, oldKey)

	t.Run("step1: old-key signature accepted during transition", func(t *testing.T) {
		require.NoError(t, verify(transitInterceptor, sign(oldKey)))
	})

	t.Run("step1: new-key signature also accepted during transition", func(t *testing.T) {
		require.NoError(t, verify(transitInterceptor, sign(newKey)))
	})

	// Step 2: Distributor redeployed with newKey; ingester still in transition.
	t.Run("step2: distributor now signs with new key, still accepted", func(t *testing.T) {
		require.NoError(t, verify(transitInterceptor, sign(newKey)))
	})

	// Step 3: Ingester redeployed with newKey only.
	finalInterceptor := NewStreamSigningServerInterceptor(newKey)

	t.Run("step3: new-key signature accepted after rotation complete", func(t *testing.T) {
		require.NoError(t, verify(finalInterceptor, sign(newKey)))
	})

	t.Run("step3: old-key signature rejected after rotation complete", func(t *testing.T) {
		err := verify(finalInterceptor, sign(oldKey))
		require.ErrorIs(t, err, ErrSignatureMismatch)
	})
}
