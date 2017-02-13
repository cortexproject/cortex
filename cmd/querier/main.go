package main

import (
	"flag"
	"fmt"
	"net/http"

	"golang.org/x/net/context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/retrieval"
	"github.com/prometheus/prometheus/web/api/v1"

	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/chunk"
	"github.com/weaveworks/cortex/distributor"
	"github.com/weaveworks/cortex/querier"
	"github.com/weaveworks/cortex/ring"
	"github.com/weaveworks/cortex/server"
	"github.com/weaveworks/cortex/util"
)

type dummyTargetRetriever struct{}

func (r dummyTargetRetriever) Targets() []*retrieval.Target { return nil }

type dummyAlertmanagerRetriever struct{}

func (r dummyAlertmanagerRetriever) Alertmanagers() []string { return nil }

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
	defer dist.Stop()
	prometheus.MustRegister(dist)

	server := server.New(serverConfig, r)
	defer server.Stop()

	chunkStore, err := chunk.NewStore(chunkStoreConfig)
	if err != nil {
		log.Fatal(err)
	}

	queryable := querier.NewQueryable(dist, chunkStore)
	engine := promql.NewEngine(queryable, nil)
	api := v1.NewAPI(engine, querier.DummyStorage{Queryable: queryable}, dummyTargetRetriever{}, dummyAlertmanagerRetriever{})
	promRouter := route.New(func(r *http.Request) (context.Context, error) {
		userID := r.Header.Get(user.OrgIDHeaderName)
		if userID == "" {
			return nil, fmt.Errorf("no %s header", user.OrgIDHeaderName)
		}
		return user.WithID(r.Context(), userID), nil
	}).WithPrefix("/api/prom/api/v1")
	api.Register(promRouter)

	subrouter := server.HTTP.PathPrefix("/api/prom").Subrouter()
	subrouter.PathPrefix("/api/v1").Handler(promRouter)
	subrouter.Path("/validate_expr").Handler(http.HandlerFunc(dist.ValidateExprHandler))
	subrouter.Path("/user_stats").Handler(http.HandlerFunc(dist.UserStatsHandler))

	server.Run()
}
