package main

import (
	"context"
	"flag"
	"os"

	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/migrate"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/go-kit/kit/log/level"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"google.golang.org/grpc"
)

func main() {
	var (
		serverConfig = server.Config{
			MetricsNamespace: "cortex",
			GRPCMiddleware: []grpc.UnaryServerInterceptor{
				middleware.ServerUserHeaderInterceptor,
			},
			ExcludeRequestInLog: true,
		}
		storageConfig storage.Config
		readerConfig  migrate.ReaderConfig
	)
	flagext.RegisterFlags(&storageConfig, &readerConfig, &serverConfig)
	flag.Parse()

	util.InitLogger(&serverConfig)

	server, err := server.New(serverConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing server", "err", err)
		os.Exit(1)
	}
	defer server.Shutdown()

	go server.Run()

	store, err := migrate.NewStorage(readerConfig.StorageClient, storageConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "unable to initialize storage client", "err", err)
		os.Exit(1)
	}

	reader, err := migrate.NewReader(readerConfig, store)
	if err != nil {
		level.Error(util.Logger).Log("msg", "unable to initialize reader", "err", err)
		os.Exit(1)
	}

	err = reader.TransferData(context.Background())
	if err != nil {
		level.Error(util.Logger).Log("msg", "unable to complete transfer", "err", err)
		os.Exit(1)
	}
}
