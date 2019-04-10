package main

import (
	"flag"
	"os"

	"github.com/go-kit/kit/log/level"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
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

		ingesterConfig ingester.Config
		storageConfig  storage.Config
		schemaConfig   chunk.SchemaConfig
		tbmConfig      chunk.TableManagerConfig
	)

	// Setting the environment variable JAEGER_AGENT_HOST enables tracing
	trace := tracing.NewFromEnv("ingester")
	defer trace.Close()

	flagext.RegisterFlags(&ingesterConfig, &serverConfig, &storageConfig, &schemaConfig, &tbmConfig)
	flag.Parse()

	util.InitLogger(&serverConfig)

	err := schemaConfig.Load()
	util.CheckFatal("loading schema config", err)
	// Assume the newest config is the one to use
	lastConfig := &schemaConfig.Configs[len(schemaConfig.Configs)-1]

	if (tbmConfig.ChunkTables.WriteScale.Enabled ||
		tbmConfig.IndexTables.WriteScale.Enabled ||
		tbmConfig.ChunkTables.InactiveWriteScale.Enabled ||
		tbmConfig.IndexTables.InactiveWriteScale.Enabled ||
		tbmConfig.ChunkTables.ReadScale.Enabled ||
		tbmConfig.IndexTables.ReadScale.Enabled ||
		tbmConfig.ChunkTables.InactiveReadScale.Enabled ||
		tbmConfig.IndexTables.InactiveReadScale.Enabled) &&
		(storageConfig.AWSStorageConfig.ApplicationAutoScaling.URL == nil && storageConfig.AWSStorageConfig.Metrics.URL == "") {
		level.Error(util.Logger).Log("msg", "WriteScale is enabled but no ApplicationAutoScaling or Metrics URL has been provided")
		os.Exit(1)
	}

	tableClient, err := storage.NewTableClient(lastConfig.IndexType, storageConfig)
	util.CheckFatal("initializing table client", err)

	tableManager, err := chunk.NewTableManager(tbmConfig, schemaConfig, ingesterConfig.MaxChunkAge, tableClient)
	util.CheckFatal("initializing table manager", err)
	tableManager.Start()
	defer tableManager.Stop()

	server, err := server.New(serverConfig)
	util.CheckFatal("initializing server", err)
	defer server.Shutdown()

	server.Run()
}
