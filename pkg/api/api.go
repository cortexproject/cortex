package api

import (
	"errors"
	"flag"
	"net/http"
	"regexp"

	"github.com/go-kit/kit/log"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/cortexproject/cortex/pkg/alertmanager"
	"github.com/cortexproject/cortex/pkg/chunk/purger"
	"github.com/cortexproject/cortex/pkg/distributor"
	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/ruler"
	"github.com/cortexproject/cortex/pkg/util/push"
)

type Config struct {
	AlertmanagerHTTPPrefix string `yaml:"alertmanager_http_prefix"`
	PrometheusHTTPPrefix   string `yaml:"prometheus_http_prefix"`

	HTTPAuthMiddleware middleware.Func `yaml:"-"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet with the set prefix.
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.AlertmanagerHTTPPrefix, prefix+"http.alertmanager-http-prefix", "/alertmanager", "Base path for data storage.")
	f.StringVar(&cfg.PrometheusHTTPPrefix, prefix+"http.prometheus-http-prefix", "/prometheus", "Base path for data storage.")
}

type API struct {
	cfg          Config
	legacyPrefix string

	authMiddleware     middleware.Func
	server             *server.Server
	alertmanagerRouter *mux.Router
	prometheusRouter   *mux.Router

	reg    prometheus.Registerer
	logger log.Logger
}

func New(cfg Config, s *server.Server, legacyPrefix string, logger log.Logger, reg prometheus.Registerer) (*API, error) {
	api := &API{
		cfg:                cfg,
		legacyPrefix:       legacyPrefix,
		authMiddleware:     cfg.HTTPAuthMiddleware,
		alertmanagerRouter: s.HTTP.PathPrefix(cfg.AlertmanagerHTTPPrefix).Subrouter(),
		prometheusRouter:   s.HTTP.PathPrefix(cfg.PrometheusHTTPPrefix).Subrouter(),
		server:             s,
		reg:                reg,
		logger:             logger,
	}

	if cfg.HTTPAuthMiddleware == nil {
		api.authMiddleware = middleware.AuthenticateUser
	}

	return api, nil
}

func (a *API) registerRoute(path string, handler http.Handler, auth bool, methods ...string) {
	if auth {
		handler = a.authMiddleware.Wrap(handler)
	}
	if len(methods) == 0 {
		a.server.HTTP.Path(path).Handler(handler)
		return
	}
	a.server.HTTP.Path(path).Methods(methods...).Handler(handler)
}

func (a *API) registerPrometheusRoute(path string, handler http.Handler, methods ...string) {
	if len(methods) == 0 {
		a.prometheusRouter.Path(path).Handler(fakeRemoteAddr(a.authMiddleware.Wrap(handler)))
		return
	}
	a.prometheusRouter.Path(path).Methods(methods...).Handler(fakeRemoteAddr(a.authMiddleware.Wrap(handler)))
}

func (a *API) registerAlertmanagerRoute(path string, handler http.Handler, methods ...string) {
	if len(methods) == 0 {
		a.alertmanagerRouter.Path(path).Handler(fakeRemoteAddr(a.authMiddleware.Wrap(handler)))
		return
	}
	a.alertmanagerRouter.Path(path).Methods(methods...).Handler(fakeRemoteAddr(a.authMiddleware.Wrap(handler)))
}

// Latest Prometheus requires r.RemoteAddr to be set to addr:port, otherwise it reject the request.
// Requests to Querier sometimes doesn't have that (if they are fetched from Query-Frontend).
// Prometheus uses this when logging queries to QueryLogger, but Cortex doesn't call engine.SetQueryLogger to set one.
//
// Can be removed when (if) https://github.com/prometheus/prometheus/pull/6840 is merged.
func fakeRemoteAddr(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.RemoteAddr == "" {
			r.RemoteAddr = "127.0.0.1:8888"
		}
		handler.ServeHTTP(w, r)
	})
}

func (a *API) RegisterAlertmanager(am *alertmanager.MultitenantAlertmanager) {
	a.RegisterRoute("/alertmanager/status", am.GetStatusHandler(), false)

}

func (a *API) RegisterDistributor(d *distributor.Distributor, pushConfig distributor.Config) {
	a.registerRoute("/all_user_stats", http.HandlerFunc(d.AllUserStatsHandler), false)
	a.registerRoute("/push", push.Handler(pushConfig, d.Push), true)
	a.registerRoute("/ha-tracker", d.Replicas, false)
}

func (a *API) RegisterIngester(i *ingester.Ingester, pushConfig distributor.Config) {
	client.RegisterIngesterServer(a.server.GRPC, i)
	grpc_health_v1.RegisterHealthServer(a.server.GRPC, i)
	a.registerRoute("/flush", http.HandlerFunc(i.FlushHandler), false)
	a.registerRoute("/shutdown", http.HandlerFunc(i.ShutdownHandler), false)
	a.registerRoute("/push", push.Handler(pushConfig, i.Push), true)
}

func (a *API) RegisterPurger(store *purger.DeleteStore) error {
	var deleteRequestHandler *purger.DeleteRequestHandler
	deleteRequestHandler, err := purger.NewDeleteRequestHandler(store)
	if err != nil {
		return err
	}

	a.registerPrometheusRoute("/api/v1/admin/tsdb/delete_series", http.HandlerFunc(deleteRequestHandler.AddDeleteRequestHandler), "PUT", "POST")
	a.registerPrometheusRoute("/api/v1/admin/tsdb/delete_series", http.HandlerFunc(deleteRequestHandler.GetAllDeleteRequestsHandler), "GET")
	return nil
}

func (a *API) RegisterRing(component string, handler http.Handler) {
	a.server.HTTP.Handle("/"+component+"/ring", handler)
}

func (a *API) RegisterRuler(r *ruler.Ruler) {
	a.registerPrometheusRoute("/api/v1/rules", http.HandlerFunc(r.PrometheusRules), "GET")
	a.registerPrometheusRoute("/api/v1/alerts", http.HandlerFunc(r.PrometheusAlerts), "GET")
	a.registerRoute("/api/v1/rules", http.HandlerFunc(r.ListRules), true, "GET")
	a.registerRoute("/api/v1/rules/{namespace}", http.HandlerFunc(r.ListRules), true, "GET")
	a.registerRoute("/api/v1/rules/{namespace}/{groupName}", http.HandlerFunc(r.GetRuleGroup), true, "GET")
	a.registerRoute("/api/v1/rules/{namespace}/{groupName}", http.HandlerFunc(r.GetRuleGroup), true, "GET")
	a.registerRoute("/api/v1/rules/{namespace}", http.HandlerFunc(r.CreateRuleGroup), true, "POST")
	a.registerRoute("/api/v1/rules/{namespace}/{groupName}", http.HandlerFunc(r.DeleteRuleGroup), true, "DELETE")
}

func (a *API) RegisterQuerier(queryable storage.Queryable, engine *promql.Engine, distributor *distributor.Distributor) error {
	api := v1.NewAPI(
		engine,
		queryable,
		querier.DummyTargetRetriever{},
		querier.DummyAlertmanagerRetriever{},
		func() config.Config { return config.Config{} },
		map[string]string{}, // TODO: include configuration flags
		func(f http.HandlerFunc) http.HandlerFunc { return f },
		func() v1.TSDBAdmin { return nil }, // Only needed for admin APIs.
		false,                              // Disable admin APIs.
		a.logger,
		querier.DummyRulesRetriever{},
		0, 0, 0, // Remote read samples and concurrency limit.
		regexp.MustCompile(".*"),
		func() (v1.RuntimeInfo, error) { return v1.RuntimeInfo{}, errors.New("not implemented") },
		&v1.PrometheusVersion{},
	)

	promRouter := route.New().WithPrefix(a.cfg.PrometheusHTTPPrefix + "/api/v1")
	api.Register(promRouter)

	a.registerPrometheusRoute("/api/v1/read", querier.RemoteReadHandler(queryable))
	a.registerPrometheusRoute("/api/v1/query", promRouter)
	a.registerPrometheusRoute("/api/v1/query_range", promRouter)
	a.registerPrometheusRoute("/api/v1/labels", promRouter)
	a.registerPrometheusRoute("/api/v1/label/{name}/values", promRouter)
	a.registerPrometheusRoute("/api/v1/series", promRouter)
	a.registerPrometheusRoute("/api/v1/metadata", promRouter)

	a.registerRoute("/user_stats", http.HandlerFunc(distributor.UserStatsHandler), true)
	a.registerRoute("/api/v1/chunks", querier.ChunksHandler(queryable), true)

	return nil
}
