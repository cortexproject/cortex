package main

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/util"
)

type Proxy struct {
	cfg      Config
	backends []*ProxyBackend
	logger   log.Logger
	metrics  *ProxyMetrics
}

func NewProxy(cfg Config, registerer prometheus.Registerer) (*Proxy, error) {
	p := &Proxy{
		cfg:     cfg,
		logger:  util.Logger,
		metrics: NewProxyMetrics(registerer),
	}

	// Parse the backend endpoints (comma separated).
	parts := strings.Split(cfg.BackendEndpoints, ",")

	for _, part := range parts {
		u, err := url.Parse(part)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid backend endpoint %s", part)
		}

		// The backend name is hardcoded as the backend hostname.
		name := u.Hostname()

		p.backends = append(p.backends, NewProxyBackend(name, u, cfg.BackendReadTimeout, name == cfg.PreferredBackend))
	}

	// At least 2 backends are required
	if len(p.backends) < 2 {
		return nil, errors.New("at least 2 backends are required")
	}

	return p, nil
}

func (p *Proxy) Run() error {
	router := mux.NewRouter()

	// Read endpoints.
	router.Path("/api/prom/api/v1/query").Methods("GET").Handler(NewProxyEndpoint(p.backends, "api_v1_query", p.metrics, p.logger))
	router.Path("/api/prom/api/v1/query_range").Methods("GET").Handler(NewProxyEndpoint(p.backends, "api_v1_query_range", p.metrics, p.logger))
	router.Path("/api/prom/api/v1/labels").Methods("GET").Handler(NewProxyEndpoint(p.backends, "api_v1_labels", p.metrics, p.logger))
	router.Path("/api/prom/api/v1/label/{name}/values").Methods("GET").Handler(NewProxyEndpoint(p.backends, "api_v1_label_name_values", p.metrics, p.logger))
	router.Path("/api/prom/api/v1/series").Methods("GET").Handler(NewProxyEndpoint(p.backends, "api_v1_series", p.metrics, p.logger))

	s := http.Server{
		Addr:         fmt.Sprintf(":%d", p.cfg.ServerServicePort),
		ReadTimeout:  1 * time.Minute,
		WriteTimeout: 2 * time.Minute,
		Handler:      router,
	}

	return s.ListenAndServe()
}
