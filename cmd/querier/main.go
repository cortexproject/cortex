package main

import (
	"flag"
	"fmt"
	"net/http"

	"golang.org/x/net/context"

	"github.com/prometheus/common/log"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/web/api/v1"

	"github.com/weaveworks/cortex/chunk"
	"github.com/weaveworks/cortex/distributor"
	"github.com/weaveworks/cortex/querier"
	"github.com/weaveworks/cortex/ring"
	"github.com/weaveworks/cortex/server"
	"github.com/weaveworks/cortex/ui"
	"github.com/weaveworks/cortex/user"
	"github.com/weaveworks/cortex/util"
)

func main() {
	var (
		serverConfig      server.Config
		ringConfig        ring.Config
		distributorConfig distributor.Config
		chunkStoreConfig  chunk.StoreConfig
	)
	util.RegisterFlags(&serverConfig, &ringConfig, &distributorConfig, &chunkStoreConfig)
	flag.Parse()

	r, err := ring.New(ringConfig)
	if err != nil {
		log.Fatalf("Error initializing ring: %v", err)
	}
	defer r.Stop()

	dist, err := distributor.New(distributorConfig, r)
	if err != nil {
		log.Fatalf("Error initializing distributor: %v", err)
	}

	server := server.New(serverConfig, r)
	defer server.Stop()

	chunkStore := chunk.NewAWSStore(chunkStoreConfig)
	queryable := querier.NewQueryable(dist, chunkStore)
	engine := promql.NewEngine(queryable, nil)
	api := v1.NewAPI(engine, querier.DummyStorage{Queryable: queryable})
	promRouter := route.New(func(r *http.Request) (context.Context, error) {
		userID := r.Header.Get(user.UserIDHeaderName)
		if userID == "" {
			return nil, fmt.Errorf("no %s header", user.UserIDHeaderName)
		}
		return user.WithID(r.Context(), userID), nil
	}).WithPrefix("/api/prom/api/v1")
	api.Register(promRouter)

	subrouter := server.HTTP.PathPrefix("/api/prom").Subrouter()
	subrouter.PathPrefix("/api/v1").Handler(promRouter)
	subrouter.Path("/validate_expr").Handler(http.HandlerFunc(dist.ValidateExprHandler))
	subrouter.Path("/user_stats").Handler(http.HandlerFunc(dist.UserStatsHandler))
	subrouter.Path("/graph").Handler(ui.GraphHandler())
	subrouter.PathPrefix("/static/").Handler(ui.StaticAssetsHandler("/api/prom/static/"))

	server.Run()
}
