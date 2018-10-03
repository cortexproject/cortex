package main

import (
	"flag"
	"os"

	"github.com/go-kit/kit/log/level"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/configs/api"
	"github.com/cortexproject/cortex/pkg/configs/db"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
)

func main() {
	var (
		serverConfig = server.Config{
			MetricsNamespace: "cortex",
			// XXX: Cargo-culted from distributor. Probably don't need this
			// for configs just yet?
			GRPCMiddleware: []grpc.UnaryServerInterceptor{
				middleware.ServerUserHeaderInterceptor,
			},
		}
		dbConfig db.Config
	)
	util.RegisterFlags(&serverConfig, &dbConfig)
	flag.Parse()

	util.InitLogger(&serverConfig)

	db, err := db.New(dbConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing database", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	a := api.New(db)

	server, err := server.New(serverConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing server", "err", err)
		os.Exit(1)
	}
	defer server.Shutdown()

	a.RegisterRoutes(server.HTTP)
	server.Run()
}
