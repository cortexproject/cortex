package main

import (
	"flag"
	"net/http"
	"os"

	"github.com/go-kit/kit/log/level"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip" // get gzip compressor registered
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/profiletrigger"
	"github.com/cortexproject/cortex/pkg/util/validation"
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
			GRPCStreamMiddleware: []grpc.StreamServerInterceptor{
				func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
					// Don't check auth header on TransferChunks, as we weren't originally
					// sending it and this could cause transfers to fail on update.
					if info.FullMethod == "/cortex.Ingester/TransferChunks" {
						return handler(srv, ss)
					}

					return middleware.StreamServerUserHeaderInterceptor(srv, ss, info, handler)
				},
			},
			ExcludeRequestInLog: true,
		}
		chunkStoreConfig chunk.StoreConfig
		schemaConfig     chunk.SchemaConfig
		storageConfig    storage.Config
		ingesterConfig   ingester.Config
		preallocConfig   client.PreallocConfig
		clientConfig     client.Config
		limits           validation.Limits
		profiletrigger   profiletrigger.Config
		eventSampleRate  int
		maxStreams       uint
	)

	// Setting the environment variable JAEGER_AGENT_HOST enables tracing
	trace := tracing.NewFromEnv("ingester")
	defer trace.Close()

	// Ingester needs to know our gRPC listen port.
	ingesterConfig.LifecyclerConfig.ListenPort = &serverConfig.GRPCListenPort

	util.RegisterFlags(&serverConfig, &chunkStoreConfig, &storageConfig,
		&schemaConfig, &ingesterConfig, &clientConfig, &limits, &preallocConfig,
		&profiletrigger)
	flag.UintVar(&maxStreams, "ingester.max-concurrent-streams", 1000, "Limit on the number of concurrent streams for gRPC calls (0 = unlimited)")
	flag.IntVar(&eventSampleRate, "event.sample-rate", 0, "How often to sample observability events (0 = never).")
	flag.Parse()

	util.InitLogger(&serverConfig)
	util.InitEvents(eventSampleRate)
	profiletrigger.Run()

	if maxStreams > 0 {
		serverConfig.GRPCOptions = append(serverConfig.GRPCOptions, grpc.MaxConcurrentStreams(uint32(maxStreams)))
	}

	server, err := server.New(serverConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing server", "err", err)
		os.Exit(1)
	}
	defer server.Shutdown()

	overrides, err := validation.NewOverrides(limits)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing overrides", "err", err)
		os.Exit(1)
	}
	chunkStore, err := storage.NewStore(storageConfig, chunkStoreConfig, schemaConfig, overrides)
	if err != nil {
		level.Error(util.Logger).Log("err", err)
		os.Exit(1)
	}
	defer chunkStore.Stop()

	ingester, err := ingester.New(ingesterConfig, clientConfig, overrides, chunkStore)
	if err != nil {
		level.Error(util.Logger).Log("err", err)
		os.Exit(1)
	}
	defer ingester.Shutdown()

	client.RegisterIngesterServer(server.GRPC, ingester)
	grpc_health_v1.RegisterHealthServer(server.GRPC, ingester)
	server.HTTP.Path("/ready").Handler(http.HandlerFunc(ingester.ReadinessHandler))
	server.HTTP.Path("/flush").Handler(http.HandlerFunc(ingester.FlushHandler))
	server.Run()
}
