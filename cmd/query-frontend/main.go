package main

import (
	"flag"
	"os"

	"github.com/go-kit/kit/log/level"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/querier/frontend"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/common/tracing"
)

func main() {
	var (
		serverConfig = server.Config{
			MetricsNamespace: "cortex",
			GRPCMiddleware: []grpc.UnaryServerInterceptor{
				middleware.ServerUserHeaderInterceptor,
			},
		}
		frontendConfig frontend.Config
		maxMessageSize int
	)
	util.RegisterFlags(&serverConfig, &frontendConfig)
	flag.IntVar(&maxMessageSize, "query-frontend.max-recv-message-size-bytes", 1024*1024*64, "Limit on the size of a grpc message this server can receive.")
	flag.Parse()

	// Setting the environment variable JAEGER_AGENT_HOST enables tracing
	trace := tracing.NewFromEnv("query-frontend")
	defer trace.Close()

	util.InitLogger(&serverConfig)

	serverConfig.GRPCOptions = append(serverConfig.GRPCOptions, grpc.MaxRecvMsgSize(maxMessageSize))
	server, err := server.New(serverConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing server", "err", err)
		os.Exit(1)
	}
	defer server.Shutdown()

	f, err := frontend.New(frontendConfig, util.Logger)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing frontend", "err", err)
		os.Exit(1)
	}
	defer f.Close()

	frontend.RegisterFrontendServer(server.GRPC, f)
	server.HTTP.PathPrefix("/api/prom").Handler(middleware.AuthenticateUser.Wrap(f))
	server.Run()
}
