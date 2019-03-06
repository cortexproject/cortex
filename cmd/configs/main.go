package main

import (
	"flag"

	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/configs/api"
	"github.com/cortexproject/cortex/pkg/configs/db"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
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
	flagext.RegisterFlags(&serverConfig, &dbConfig)
	flag.Parse()

	util.InitLogger(&serverConfig)

	db, err := db.New(dbConfig)
	util.CheckFatal("initializing database", err)
	defer db.Close()

	a := api.New(db)

	server, err := server.New(serverConfig)
	util.CheckFatal("initializing server", err)
	defer server.Shutdown()

	a.RegisterRoutes(server.HTTP)
	server.Run()
}
