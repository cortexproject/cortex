package main

import (
	"flag"
	"net/http"

	"github.com/opentracing-contrib/go-stdlib/nethttp"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/distributor"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/common/tracing"
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
		clientConfig      client.Config
		limits            validation.Limits
		preallocConfig    client.PreallocConfig
	)
	flagext.RegisterFlags(&serverConfig, &ringConfig, &distributorConfig, &clientConfig, &limits,
		&preallocConfig)
	flag.Parse()

	util.InitLogger(&serverConfig)

	// Setting the environment variable JAEGER_AGENT_HOST enables tracing
	trace := tracing.NewFromEnv("distributor")
	defer trace.Close()

	r, err := ring.New(ringConfig)
	util.CheckFatal("initializing ring", err)
	prometheus.MustRegister(r)
	defer r.Stop()

	overrides, err := validation.NewOverrides(limits)
	util.CheckFatal("initializing overrides", err)

	dist, err := distributor.New(distributorConfig, clientConfig, overrides, r)
	util.CheckFatal("initializing distributor", err)
	defer dist.Stop()

	server, err := server.New(serverConfig)
	util.CheckFatal("initializing server", err)
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
