package middleware

import (
	"fmt"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/weaveworks/cortex/user"
)

// ClientUserHeaderInterceptor propagates the user ID from the context to gRPC metadata, which eventually ends up as a HTTP2 header.
func ClientUserHeaderInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	userID, err := user.GetID(ctx)
	if err != nil {
		return err
	}

	md, ok := metadata.FromContext(ctx)
	if !ok {
		md = metadata.New(map[string]string{user.LowerUserIDHeaderName: userID})
	} else {
		md[user.LowerUserIDHeaderName] = []string{userID}
	}
	newCtx := metadata.NewContext(ctx, md)

	return invoker(newCtx, method, req, reply, cc, opts...)
}

// ServerUserHeaderInterceptor propagates the user ID from the gRPC metadata back to our context.
func ServerUserHeaderInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	md, ok := metadata.FromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("no metadata")
	}

	userIDs, ok := md[user.LowerUserIDHeaderName]
	if !ok || len(userIDs) != 1 {
		return nil, fmt.Errorf("no user id")
	}

	newCtx := user.WithID(ctx, userIDs[0])
	return handler(newCtx, req)
}
