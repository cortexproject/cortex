package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	errMinBackends = errors.New("at least 1 backend is required")
)

type Proxy struct {
	cfg      Config
	backends []*ProxyBackend
	logger   log.Logger
	metrics  *ProxyMetrics

	// The HTTP server used to run the proxy service.
	srv         *http.Server
	srvListener net.Listener

	// Wait group used to wait until the server has done.
	done sync.WaitGroup
}

func NewProxy(cfg Config, logger log.Logger, registerer prometheus.Registerer) (*Proxy, error) {
	p := &Proxy{
		cfg:     cfg,
		logger:  logger,
		metrics: NewProxyMetrics(registerer),
	}

	// Parse the backend endpoints (comma separated).
	parts := strings.Split(cfg.BackendEndpoints, ",")

	for idx, part := range parts {
		// Skip empty ones.
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		u, err := url.Parse(part)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid backend endpoint %s", part)
		}

		// The backend name is hardcoded as the backend hostname.
		name := u.Hostname()
		preferred := name == cfg.PreferredBackend

		// In tests we have the same hostname for all backends, so we also
		// support a numeric preferred backend which is the index in the list
		// of backends.
		if preferredIdx, err := strconv.Atoi(cfg.PreferredBackend); err == nil {
			preferred = preferredIdx == idx
		}

		p.backends = append(p.backends, NewProxyBackend(name, u, cfg.BackendReadTimeout, preferred))
	}

	// At least 1 backend is required
	if len(p.backends) < 1 {
		return nil, errMinBackends
	}

	// At least 2 backends are suggested
	if len(p.backends) < 2 {
		level.Warn(p.logger).Log("msg", "The proxy is running with only 1 backend. At least 2 backends are required to fulfil the purpose of the proxy and compare results.")
	}

	return p, nil
}

func (p *Proxy) Start() error {
	// Setup listener first, so we can fail early if the port is in use.
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", p.cfg.ServerServicePort))
	if err != nil {
		return err
	}

	router := mux.NewRouter()

	// Health check endpoint.
	router.Path("/").Methods("GET").Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Read endpoints.
	router.Path("/api/v1/query").Methods("GET").Handler(NewProxyEndpoint(p.backends, "api_v1_query", p.metrics, p.logger))
	router.Path("/api/v1/query_range").Methods("GET").Handler(NewProxyEndpoint(p.backends, "api_v1_query_range", p.metrics, p.logger))
	router.Path("/api/v1/labels").Methods("GET").Handler(NewProxyEndpoint(p.backends, "api_v1_labels", p.metrics, p.logger))
	router.Path("/api/v1/label/{name}/values").Methods("GET").Handler(NewProxyEndpoint(p.backends, "api_v1_label_name_values", p.metrics, p.logger))
	router.Path("/api/v1/series").Methods("GET").Handler(NewProxyEndpoint(p.backends, "api_v1_series", p.metrics, p.logger))

	p.srvListener = listener
	p.srv = &http.Server{
		ReadTimeout:  1 * time.Minute,
		WriteTimeout: 2 * time.Minute,
		Handler:      router,
	}

	// Run in a dedicated goroutine.
	p.done.Add(1)
	go func() {
		defer p.done.Done()

		if err := p.srv.Serve(p.srvListener); err != nil {
			level.Error(p.logger).Log("msg", "Proxy server failed", "err", err)
		}
	}()

	return nil
}

func (p *Proxy) Stop() error {
	if p.srv == nil {
		return nil
	}

	return p.srv.Shutdown(context.Background())
}

func (p *Proxy) Await() {
	// Wait until terminated.
	p.done.Wait()
}

func (p *Proxy) Endpoint() string {
	if p.srvListener == nil {
		return ""
	}

	return p.srvListener.Addr().String()
}
