package channelz

import (
	"net/http"
	"path"
	"sync"

	"google.golang.org/grpc"
	channelzgrpc "google.golang.org/grpc/channelz/grpc_channelz_v1"
	"google.golang.org/grpc/credentials/insecure"
)

// CreateHandler creates an http handler with the routes of channelz mounted to the provided prefix.
// pathPrefix is the prefix to which /channelz will be prepended
// grpcBindAddress is the TCP bind address for the gRPC service you'd like to monitor.
// grpcBindAddress is required since the channelz interface connects to this gRPC service.
// Typically you'd use the return value of CreateHandler as an argument to http.Handle
// For example:
//
// http.Handle("/", channelz.CreateHandler("/foo", grpcBindAddress))
//
// grpc.Dial is called using grpc.WithTransportCredentials(insecure.NewCredentials()).
// If you need custom DialOptions like credentials, TLS or interceptors, please
// refer to CreateHandlerWithDialOpts().
func CreateHandler(pathPrefix, grpcBindAddress string) http.Handler {
	return CreateHandlerWithDialOpts(
		pathPrefix,
		grpcBindAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}

// CreateHandlerWithDialOpts is the same as CreateHandler but with custom []grpc.DialOption
// You need to provide all grpc.DialOption to be used for the internal call to grpc.Dial().
// This typically includes some form of grpc.WithTransportCredentials().
// Here's an example on how to use a bufconn instead of a real TCP listener:
// lis := bufconn.Listen(1024 * 1024)
// grpcserver.Serve(lis)
// http.Handle("/", channelzWeb.CreateHandlerWithDialOpts("/", "",
//
//	[]grpc.DialOption{
//		grpc.WithTransportCredentials(insecure.NewCredentials()),
//		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
//			return lis.DialContext(ctx)
//		}),
//	}...,
//
// ))
func CreateHandlerWithDialOpts(pathPrefix, grpcBindAddress string, dialOpts ...grpc.DialOption) http.Handler {
	prefix := path.Join(pathPrefix, "channelz") + "/"
	handler := &grpcChannelzHandler{
		bindAddress: grpcBindAddress,
		dialOpts:    dialOpts,
	}
	return createRouter(prefix, handler)
}

type grpcChannelzHandler struct {
	// the target server's bind address
	bindAddress string

	// The client connection (lazily initialized)
	client channelzgrpc.ChannelzClient

	// []grpc.DialOption to use for grpc.Dial
	dialOpts []grpc.DialOption

	mu sync.Mutex
}
