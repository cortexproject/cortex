package main

import (
	"flag"
	"net/http"
	"os"

	"github.com/go-kit/kit/log/level"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"

	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/common/tracing"
	"github.com/weaveworks/cortex/pkg/distributor"
	"github.com/weaveworks/cortex/pkg/ring"
	"github.com/weaveworks/cortex/pkg/util"
)

func main() {
	// The pattern for main functions is a series of config objects, which are
	// registered for command line flags, and then a series of components that
	// are instantiated and composed.  Some rules of thumb:
	// - Config types should only contain 'simple' types (ints, strings, urls etc).
	// - Flag validation should be done by the flag; use a flag.Value where
	//   appropriate.
	// - Config types should map 1:1 with a component type.
	// - Config types should define flags with a common prefix.
	// - It's fine to nest configs within configs, but this should match the
	//   nesting of components within components.
	// - Limit as much is possible sharing of configuration between config types.
	//   Where necessary, use a pointer for this - avoid repetition.
	// - Where a nesting of components its not obvious, it's fine to pass
	//   references to other components constructors to compose them.
	// - First argument for a components constructor should be its matching config
	//   object.

	var (
		serverConfig = server.Config{
			MetricsNamespace: "cortex",
			GRPCMiddleware: []grpc.UnaryServerInterceptor{
				middleware.ServerUserHeaderInterceptor,
			},
			ExcludeRequestInLog: true,
		}
		ringConfig        ring.Config
		distributorConfig distributor.Config
		logLevel          util.LogLevel
	)
	util.RegisterFlags(&serverConfig, &ringConfig, &distributorConfig, &logLevel)
	flag.Parse()

	util.InitLogger(logLevel.AllowedLevel)

	// Setting the environment variable JAEGER_AGENT_HOST enables tracing
	trace := tracing.NewFromEnv("distributor")
	defer trace.Close()

	r, err := ring.New(ringConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing ring", "err", err)
		os.Exit(1)
	}
	defer r.Stop()

	dist, err := distributor.New(distributorConfig, r)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing distributor", "err", err)
		os.Exit(1)
	}
	defer dist.Stop()
	prometheus.MustRegister(dist)

	server, err := server.New(serverConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing server", "err", err)
		os.Exit(1)
	}
	defer server.Shutdown()

	// Administrator functions
	server.HTTP.Handle("/ring", r)
	server.HTTP.HandleFunc("/all_user_stats", dist.AllUserStatsHandler)

	operationNameFunc := nethttp.OperationNameFunc(func(r *http.Request) string {
		return r.URL.RequestURI()
	})
	server.HTTP.Handle("/api/prom/push", middleware.Merge(
		middleware.Func(func(handler http.Handler) http.Handler {
			return nethttp.Middleware(opentracing.GlobalTracer(), handler, operationNameFunc)
		}),
		middleware.AuthenticateUser,
	).Wrap(http.HandlerFunc(dist.PushHandler)))

	server.Run()
}
