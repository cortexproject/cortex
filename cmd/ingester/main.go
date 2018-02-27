package main

import (
	"flag"
	"net/http"
	"os"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip" // get gzip compressor registered

	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/common/tracing"
	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/chunk/storage"
	"github.com/weaveworks/cortex/pkg/ingester"
	"github.com/weaveworks/cortex/pkg/ingester/client"
	"github.com/weaveworks/cortex/pkg/util"
)

func main() {
	var (
		serverConfig = server.Config{
			MetricsNamespace: "cortex",
			GRPCMiddleware: []grpc.UnaryServerInterceptor{
				middleware.ServerUserHeaderInterceptor,
			},
		}
		chunkStoreConfig chunk.StoreConfig
		schemaConfig     chunk.SchemaConfig
		storageConfig    storage.Config
		ingesterConfig   ingester.Config
		logLevel         util.LogLevel
		maxStreams       uint
	)

	// Setting the environment variable JAEGER_AGENT_HOST enables tracing
	trace := tracing.NewFromEnv("ingester")
	defer trace.Close()

	// Ingester needs to know our gRPC listen port.
	ingesterConfig.ListenPort = &serverConfig.GRPCListenPort
	util.RegisterFlags(&serverConfig, &chunkStoreConfig, &storageConfig,
		&schemaConfig, &ingesterConfig, &logLevel)
	flag.UintVar(&maxStreams, "ingester.max-concurrent-streams", 1000, "Limit on the number of concurrent streams for gRPC calls (0 = unlimited)")
	flag.Parse()

	util.InitLogger(logLevel.AllowedLevel)

	if maxStreams > 0 {
		serverConfig.GRPCOptions = append(serverConfig.GRPCOptions, grpc.MaxConcurrentStreams(uint32(maxStreams)))
	}

	server, err := server.New(serverConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing server", "err", err)
		os.Exit(1)
	}
	defer server.Shutdown()

	storageClient, err := storage.NewStorageClient(storageConfig, schemaConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing storage client", "err", err)
		os.Exit(1)
	}

	chunkStore, err := chunk.NewStore(chunkStoreConfig, schemaConfig, storageClient)
	if err != nil {
		level.Error(util.Logger).Log("err", err)
		os.Exit(1)
	}
	defer chunkStore.Stop()

	ingester, err := ingester.New(ingesterConfig, chunkStore)
	if err != nil {
		level.Error(util.Logger).Log("err", err)
		os.Exit(1)
	}
	prometheus.MustRegister(ingester)
	defer ingester.Shutdown()

	client.RegisterIngesterServer(server.GRPC, ingester)
	server.HTTP.Path("/ready").Handler(http.HandlerFunc(ingester.ReadinessHandler))
	server.HTTP.Path("/flush").Handler(http.HandlerFunc(ingester.FlushHandler))
	server.Run()
}
