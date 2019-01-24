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
	flagext.RegisterFlags(&ingesterConfig, &serverConfig, &storageConfig, &schemaConfig, &tbmConfig)
	flag.Parse()

	util.InitLogger(&serverConfig)

	err := schemaConfig.Load()
	if err != nil {
		level.Error(util.Logger).Log("msg", "error loading schema config", "err", err)
		os.Exit(1)
	}
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
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing DynamoDB table client", "err", err)
		os.Exit(1)
	}

	tableManager, err := chunk.NewTableManager(tbmConfig, schemaConfig, ingesterConfig.MaxChunkAge, tableClient)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing DynamoDB table manager", "err", err)
		os.Exit(1)
	}
	tableManager.Start()
	defer tableManager.Stop()

	server, err := server.New(serverConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing server", "err", err)
		os.Exit(1)
	}
	defer server.Shutdown()

	server.Run()
}
