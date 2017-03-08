package main

import (
	"flag"

	"github.com/prometheus/common/log"
	"google.golang.org/grpc"

	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/cortex/configs/api"
	"github.com/weaveworks/cortex/configs/db"
	"github.com/weaveworks/cortex/util"
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
		dbConfig  db.Config
		apiConfig api.Config
	)
	util.RegisterFlags(&serverConfig, &dbConfig, &apiConfig)
	flag.Parse()

	db, err := db.New(dbConfig)
	if err != nil {
		log.Fatalf("Error initializing database: %v", err)
	}
	defer db.Close()

	a := api.New(apiConfig, db)

	server, err := server.New(serverConfig)
	if err != nil {
		log.Fatalf("Error initializing server: %v", err)
	}
	defer server.Shutdown()

	server.HTTP.Handle("/", a)
	server.Run()
}
