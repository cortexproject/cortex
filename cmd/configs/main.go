package main

import (
	"flag"
	"fmt"
	"net/http"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tylerb/graceful"

	"github.com/weaveworks/common/logging"
	"github.com/weaveworks/service/common"
	"github.com/weaveworks/service/configs/api"
	"github.com/weaveworks/service/configs/db"
)

func init() {
	prometheus.MustRegister(common.RequestDuration)
	prometheus.MustRegister(common.DatabaseRequestDuration)
}

func main() {
	var (
		logLevel           = flag.String("log.level", "info", "Logging level to use: debug | info | warn | error")
		port               = flag.Int("port", 80, "port to listen on")
		stopTimeout        = flag.Duration("stop.timeout", 5*time.Second, "How long to wait for remaining requests to finish during shutdown")
		databaseURI        = flag.String("database-uri", "postgres://postgres@configs-db.weave.local/configs?sslmode=disable", "URI where the database can be found (for dev you can use memory://)")
		databaseMigrations = flag.String("database-migrations", "", "Path where the database migration files can be found")
	)
	flag.Parse()
	if err := logging.Setup(*logLevel); err != nil {
		logrus.Fatalf("Error configuring logging: %v", err)
		return
	}

	db := db.MustNew(*databaseURI, *databaseMigrations)
	defer db.Close()

	logrus.Debug("Debug logging enabled")
	logrus.Infof("Listening on port %d", *port)
	mux := http.NewServeMux()
	config := api.Config{
		Database:     db,
		UserIDHeader: api.DefaultUserIDHeader,
		OrgIDHeader:  api.DefaultOrgIDHeader,
	}
	mux.Handle("/", api.New(config))
	mux.Handle("/metrics", prometheus.Handler())

	if err := graceful.RunWithErr(fmt.Sprintf(":%d", *port), *stopTimeout, mux); err != nil {
		logrus.Fatal(err)
	}
	logrus.Info("Gracefully shut down")
}
