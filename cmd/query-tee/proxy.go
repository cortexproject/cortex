package main

import (
	"context"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/weaveworks/common/instrument"

	"github.com/cortexproject/cortex/pkg/util"
)

type Proxy struct {
	cfg      Config
	backends []*proxyBackend
	logger   log.Logger

	// Metrics.
	registry       *prometheus.Registry
	durationMetric *prometheus.HistogramVec
}

type proxyBackend struct {
	cfg      BackendConfig
	name     string
	endpoint *url.URL
	client   *http.Client
}

type proxyResponse struct {
	status int
	body   []byte
}

type backendResponse struct {
	backendName string
	res         *proxyResponse
	err         error
	elapsed     time.Duration
}

func NewProxy(cfg Config) *Proxy {
	// Init backends.
	backends := make([]*proxyBackend, 0, len(cfg.Backends))
	for _, b := range cfg.Backends {
		backends = append(backends, &proxyBackend{
			cfg:      b,
			name:     b.name,
			endpoint: &b.endpoint,
			client: &http.Client{
				CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
					return errors.New("The query-tee proxy does not follow redirects.")
				},
				Transport: &http.Transport{
					Proxy: http.ProxyFromEnvironment,
					DialContext: (&net.Dialer{
						Timeout:   30 * time.Second,
						KeepAlive: 30 * time.Second,
						DualStack: true,
					}).DialContext,
					MaxIdleConns:          10000,
					MaxIdleConnsPerHost:   1000, // see https://github.com/golang/go/issues/13801
					IdleConnTimeout:       90 * time.Second,
					DisableKeepAlives:     true,
					TLSHandshakeTimeout:   10 * time.Second,
					ExpectContinueTimeout: 1 * time.Second,
				},
			},
		})
	}

	p := &Proxy{
		cfg:      cfg,
		backends: backends,
		registry: prometheus.NewRegistry(),
		logger:   util.Logger,
		durationMetric: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "cortex_querytee",
			Name:      "request_duration_seconds",
			Help:      "Time (in seconds) spent serving HTTP requests.",
			Buckets:   instrument.DefBuckets,
		}, []string{"backend", "method", "path", "status_code"}),
	}

	// Register all metrics.
	p.registry.MustRegister(p.durationMetric, prometheus.NewGoCollector())

	return p
}

func (p *Proxy) Run() error {
	router := mux.NewRouter()
	p.RegisterRoutes(router)

	s := http.Server{
		Addr:         p.cfg.ServerAddr,
		ReadTimeout:  1 * time.Minute,
		WriteTimeout: 2 * time.Minute,
		Handler:      router,
	}

	return s.ListenAndServe()
}

// RegisterRoutes registers proxy routes.
func (p *Proxy) RegisterRoutes(router *mux.Router) {
	// Read endpoints.
	router.Path("/api/prom/api/v1/query").Methods("GET", "POST").Handler(p)
	router.Path("/api/prom/api/v1/query_range").Methods("GET", "POST").Handler(p)
	router.Path("/api/prom/api/v1/labels").Methods("GET", "POST").Handler(p)
	router.Path("/api/prom/api/v1/label/{name}/values").Methods("GET", "POST").Handler(p)
	router.Path("/api/prom/api/v1/series").Methods("GET", "POST").Handler(p)

	// Metrics endpoint.
	router.Path("/metrics").Methods("GET").Handler(promhttp.HandlerFor(p.registry, promhttp.HandlerOpts{}))
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	level.Debug(p.logger).Log("msg", "Received request", "path", r.URL.Path, "query", r.URL.RawQuery)

	// Send the same request to all backends.
	wg := sync.WaitGroup{}
	wg.Add(len(p.backends))
	resCh := make(chan *backendResponse, len(p.backends))

	for _, b := range p.backends {
		b := b

		go func() {
			defer wg.Done()

			start := time.Now()
			req := p.createBackendRequest(r, b.cfg)
			res, err := p.doBackendRequest(b.client, req)
			elapsed := time.Now().Sub(start)

			resCh <- &backendResponse{
				backendName: b.cfg.name,
				res:         res,
				err:         err,
				elapsed:     elapsed,
			}

			// Log with a level based on the backend response.
			lvl := level.Debug
			if err != nil || res.status >= 500 {
				lvl = level.Warn
			}

			lvl(p.logger).Log("msg", "Backend response", "path", r.URL.Path, "query", r.URL.RawQuery, "backend", b.cfg.name, "status", res.status, "elapsed", elapsed)
		}()
	}

	// Wait until all backend requests completed.
	wg.Wait()
	close(resCh)

	// Collect all responses and track metrics for each of them.
	responses := make([]*backendResponse, 0, len(p.backends))
	for res := range resCh {
		responses = append(responses, res)

		status := 500
		if res.err == nil {
			status = res.res.status
		}

		p.durationMetric.WithLabelValues(res.backendName, r.Method, r.URL.Path, strconv.Itoa(status)).Observe(res.elapsed.Seconds())
	}

	// Select the response to send back to the client.
	downstreamRes := p.pickResponseForDownstream(responses)
	if downstreamRes.err != nil {
		http.Error(w, downstreamRes.err.Error(), http.StatusInternalServerError)
	} else {
		w.WriteHeader(downstreamRes.res.status)
		if _, err := w.Write(downstreamRes.res.body); err != nil {
			level.Warn(p.logger).Log("msg", "Unable to write response", "err", err)
		}
	}
}

func (p *Proxy) createBackendRequest(r *http.Request, cfg BackendConfig) *http.Request {
	// Clone the incoming request.
	req := r.Clone(context.Background())
	req.RequestURI = ""
	req.URL.User = nil

	// Replace the endpoint with the backend one.
	req.URL.Scheme = cfg.endpoint.Scheme
	req.URL.Host = cfg.endpoint.Host

	// Replace the auth:
	// - If the endpoint has user and password, use it.
	// - If the endpoint has user only, keep it and use the request password (if any).
	// - If the endpoint has no user and no password, use the request auth (if any).
	clientUser, clientPass, clientAuth := req.BasicAuth()
	endpointUser := cfg.endpoint.User.Username()
	endpointPass, _ := cfg.endpoint.User.Password()

	if endpointUser != "" && endpointPass != "" {
		req.SetBasicAuth(endpointUser, endpointPass)
	} else if endpointUser != "" {
		req.SetBasicAuth(endpointUser, clientPass)
	} else if clientAuth {
		req.SetBasicAuth(clientUser, clientPass)
	} else {
		req.Header.Del("Authorization")
	}

	return req
}

func (p *Proxy) doBackendRequest(client *http.Client, req *http.Request) (*proxyResponse, error) {
	// Honor the read timeout.
	ctx, cancel := context.WithTimeout(context.Background(), p.cfg.BackendReadTimeout)
	defer cancel()

	// Execute the request.
	res, err := client.Do(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "executing backend request")
	}

	// Read the entire response body.
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "reading backend response")
	}

	return &proxyResponse{
		status: res.StatusCode,
		body:   body,
	}, nil
}

func (p *Proxy) pickResponseForDownstream(responses []*backendResponse) *backendResponse {
	// Look for the preferred backend.
	var prefRes *backendResponse
	for _, res := range responses {
		if res.backendName == p.cfg.PreferredBackend {
			prefRes = res
			break
		}
	}

	// The preferred backend is fine as far as it succeeded (or 4xx).
	if prefRes != nil && prefRes.err == nil && prefRes.res.status < 500 {
		return prefRes
	}

	// Look for any other successful response (or 4xx).
	for _, res := range responses {
		if res.err == nil && res.res.status < 500 {
			return res
		}
	}

	// No successful response, so let's pick the first one.
	return responses[0]
}
