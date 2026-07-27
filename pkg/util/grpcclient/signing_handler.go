package grpcclient

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"

	"github.com/weaveworks/common/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/cortexproject/cortex/pkg/util/users"
)

var (
	reqSignHeaderName    = "x-req-signature"
	streamSignHeaderName = "x-stream-signature"
)

const (
	ErrDifferentSignaturePresent = errors.Error("different signature already present")
	ErrMultipleSignaturePresent  = errors.Error("multiples signature present")
	ErrSignatureNotPresent       = errors.Error("signature not present")
	ErrSignatureMismatch         = errors.Error("signature mismatch")

	pushStreamFullMethod = "/cortex.Ingester/PushStream"
)

// SignRequest define the interface that must be implemented by the request structs to be signed
type SignRequest interface {
	// Sign returns the signature for the given request
	Sign(context.Context) (string, error)
	// VerifySign returns true if the signature is valid
	VerifySign(context.Context, string) (bool, error)
}

func UnarySigningServerInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	rs, ok := req.(SignRequest)
	if !ok {
		return handler(ctx, req)
	}

	sig := metadata.ValueFromIncomingContext(ctx, reqSignHeaderName)
	if sig == nil || len(sig) != 1 {
		return nil, ErrSignatureNotPresent
	}

	valid, err := rs.VerifySign(ctx, sig[0])
	if err != nil {
		return nil, err
	}

	if !valid {
		return nil, ErrSignatureMismatch
	}

	return handler(ctx, req)
}

func UnarySigningClientInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	rs, ok := req.(SignRequest)

	if !ok {
		return invoker(ctx, method, req, reply, cc, opts...)
	}

	signature, err := rs.Sign(ctx)

	if err != nil {
		return err
	}

	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(map[string]string{})
	}

	newCtx := ctx

	if s, ok := md[reqSignHeaderName]; ok {
		if len(s) == 1 {
			if s[0] != signature {
				return ErrDifferentSignaturePresent
			}
		} else {
			return ErrMultipleSignaturePresent
		}
	} else {
		md = md.Copy()
		md[reqSignHeaderName] = []string{signature}
		newCtx = metadata.NewOutgoingContext(ctx, md)
	}

	return invoker(newCtx, method, req, reply, cc, opts...)
}

// computeStreamHMAC computes HMAC-SHA256(key, orgID) and returns the hex digest.
func computeStreamHMAC(key, orgID string) string {
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(orgID))
	return hex.EncodeToString(mac.Sum(nil))
}

// NewStreamSigningClientInterceptor returns a gRPC stream client interceptor that injects
// an HMAC-SHA256 signature into the outgoing stream metadata.
func NewStreamSigningClientInterceptor(key string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if method != pushStreamFullMethod {
			return streamer(ctx, desc, cc, method, opts...)
		}

		orgID, err := users.TenantID(ctx) // ingester-%s-stream-push-worker-%d
		if err != nil {
			return nil, err
		}
		sig := computeStreamHMAC(key, orgID)

		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			md = metadata.New(nil)
		} else {
			md = md.Copy()
		}
		md.Set(streamSignHeaderName, sig)

		return streamer(metadata.NewOutgoingContext(ctx, md), desc, cc, method, opts...)
	}
}

// NewStreamSigningServerInterceptor returns a gRPC stream server interceptor that verifies
// the HMAC-SHA256 signature injected by NewStreamSigningClientInterceptor.
func NewStreamSigningServerInterceptor(keys ...string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if info.FullMethod != pushStreamFullMethod {
			return handler(srv, ss)
		}

		ctx := ss.Context()
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return ErrSignatureNotPresent
		}

		sigs := md.Get(streamSignHeaderName)
		if len(sigs) != 1 {
			return ErrSignatureNotPresent
		}

		orgID, err := users.TenantID(ctx) // ingester-%s-stream-push-worker-%d
		if err != nil {
			return err
		}
		sig := sigs[0]
		for _, key := range keys {
			expectedSig := computeStreamHMAC(key, orgID)
			if subtle.ConstantTimeCompare([]byte(sig), []byte(expectedSig)) == 1 {
				return handler(srv, ss)
			}
		}
		return ErrSignatureMismatch
	}
}
