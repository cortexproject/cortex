package main

import (
	"flag"
	"os"

	"github.com/go-kit/kit/log/level"
	"google.golang.org/grpc"

	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/cortex/pkg/configs"
	"github.com/weaveworks/cortex/pkg/configs/api"
	"github.com/weaveworks/cortex/pkg/configs/db"
	"github.com/weaveworks/cortex/pkg/util"
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
		dbConfig          db.Config
		logLevel          util.LogLevel
		ruleFormatVersion configs.RuleFormatVersion
	)
	util.RegisterFlags(&serverConfig, &dbConfig, &logLevel)
	flag.Var(&ruleFormatVersion, "configs.rule-format-version", "Which Prometheus rule format version to use: '1' or '2' (default '1').")
	flag.Parse()

	util.InitLogger(logLevel.AllowedLevel)

	db, err := db.New(dbConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing database", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	a := api.New(db, ruleFormatVersion)

	server, err := server.New(serverConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing server", "err", err)
		os.Exit(1)
	}
	defer server.Shutdown()

	a.RegisterRoutes(server.HTTP)
	server.Run()
}
