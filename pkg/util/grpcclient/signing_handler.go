package grpcclient

import (
	"context"

	"github.com/weaveworks/common/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var (
	reqSignHeaderName = "x-req-signature"
)

const (
	ErrDifferentSignaturePresent = errors.Error("different signature already present")
	ErrMultipleSignaturePresent  = errors.Error("multiples signature present")
	ErrSignatureNotPresent       = errors.Error("signature not present")
	ErrSignatureMismatch         = errors.Error("signature mismatch")
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
