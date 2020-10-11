package api

import (
	"context"
	"html/template"
	"net/http"
	"path"
	"regexp"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gorilla/mux"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/weaveworks/common/instrument"
	"github.com/weaveworks/common/middleware"
	"gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/chunk/purger"
	"github.com/cortexproject/cortex/pkg/distributor"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/util"
)

const (
	SectionAdminEndpoints = "Admin Endpoints:"
	SectionDangerous      = "Dangerous:"
)

func newIndexPageContent() *IndexPageContent {
	return &IndexPageContent{
		content: map[string]map[string]string{},
	}
}

// IndexPageContent is a map of sections to path -> description.
type IndexPageContent struct {
	mu      sync.Mutex
	content map[string]map[string]string
}

func (pc *IndexPageContent) AddLink(section, path, description string) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	sectionMap := pc.content[section]
	if sectionMap == nil {
		sectionMap = make(map[string]string)
		pc.content[section] = sectionMap
	}

	sectionMap[path] = description
}

func (pc *IndexPageContent) GetContent() map[string]map[string]string {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	result := map[string]map[string]string{}
	for k, v := range pc.content {
		sm := map[string]string{}
		for smK, smV := range v {
			sm[smK] = smV
		}
		result[k] = sm
	}
	return result
}

var indexPageTemplate = ` 
<!DOCTYPE html>
<html>
	<head>
		<meta charset="UTF-8">
		<title>Cortex</title>
	</head>
	<body>
		<h1>Cortex</h1>
		{{ range $s, $links := . }}
		<p>{{ $s }}</p>
		<ul>
			{{ range $path, $desc := $links }}
				<li><a href="{{ AddPathPrefix $path }}">{{ $desc }}</a></li>
			{{ end }}
		</ul>
		{{ end }}
	</body>
</html>`

func indexHandler(httpPathPrefix string, content *IndexPageContent) http.HandlerFunc {
	templ := template.New("main")
	templ.Funcs(map[string]interface{}{
		"AddPathPrefix": func(link string) string {
			return path.Join(httpPathPrefix, link)
		},
	})
	template.Must(templ.Parse(indexPageTemplate))

	return func(w http.ResponseWriter, r *http.Request) {
		err := templ.Execute(w, content.GetContent())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func configHandler(cfg interface{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		out, err := yaml.Marshal(cfg)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/yaml")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write(out); err != nil {
			level.Error(util.Logger).Log("msg", "error writing response", "err", err)
		}
	}
}

// NewQuerierHandler returns a HTTP handler that can be used by the querier service to
// either register with the frontend worker query processor or with the external HTTP
// server to fulfill the Prometheus query API.
func NewQuerierHandler(
	cfg Config,
	queryable storage.SampleAndChunkQueryable,
	engine *promql.Engine,
	distributor *distributor.Distributor,
	tombstonesLoader *purger.TombstonesLoader,
	reg prometheus.Registerer,
	logger log.Logger,
) http.Handler {
	// Prometheus histograms for requests to the querier.
	querierRequestDuration := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "querier_request_duration_seconds",
		Help:      "Time (in seconds) spent serving HTTP requests to the querier.",
		Buckets:   instrument.DefBuckets,
	}, []string{"method", "route", "status_code", "ws"})

	receivedMessageSize := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "querier_request_message_bytes",
		Help:      "Size (in bytes) of messages received in the request to the querier.",
		Buckets:   middleware.BodySizeBuckets,
	}, []string{"method", "route"})

	sentMessageSize := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "querier_response_message_bytes",
		Help:      "Size (in bytes) of messages sent in response by the querier.",
		Buckets:   middleware.BodySizeBuckets,
	}, []string{"method", "route"})

	inflightRequests := promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "querier_inflight_requests",
		Help:      "Current number of inflight requests to the querier.",
	}, []string{"method", "route"})

	api := v1.NewAPI(
		engine,
		errorTranslateQueryable{queryable}, // Translate errors to errors expected by API.
		func(context.Context) v1.TargetRetriever { return &querier.DummyTargetRetriever{} },
		func(context.Context) v1.AlertmanagerRetriever { return &querier.DummyAlertmanagerRetriever{} },
		func() config.Config { return config.Config{} },
		map[string]string{}, // TODO: include configuration flags
		v1.GlobalURLOptions{},
		func(f http.HandlerFunc) http.HandlerFunc { return f },
		nil,   // Only needed for admin APIs.
		"",    // This is for snapshots, which is disabled when admin APIs are disabled. Hence empty.
		false, // Disable admin APIs.
		logger,
		func(context.Context) v1.RulesRetriever { return &querier.DummyRulesRetriever{} },
		0, 0, 0, // Remote read samples and concurrency limit.
		regexp.MustCompile(".*"),
		func() (v1.RuntimeInfo, error) { return v1.RuntimeInfo{}, errors.New("not implemented") },
		&v1.PrometheusVersion{},
		// This is used for the stats API which we should not support. Or find other ways to.
		prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) { return nil, nil }),
	)

	router := mux.NewRouter()

	// Use a separate metric for the querier in order to differentiate requests from the query-frontend when
	// running Cortex as a single binary.
	inst := middleware.Instrument{
		RouteMatcher:     router,
		Duration:         querierRequestDuration,
		RequestBodySize:  receivedMessageSize,
		ResponseBodySize: sentMessageSize,
		InflightRequests: inflightRequests,
	}
	cacheGenHeaderMiddleware := getHTTPCacheGenNumberHeaderSetterMiddleware(tombstonesLoader)
	middlewares := middleware.Merge(fakeRemoteAddr(), inst, cacheGenHeaderMiddleware)
	router.Use(middlewares.Wrap)

	promRouter := route.New().WithPrefix(cfg.ServerPrefix + cfg.PrometheusHTTPPrefix + "/api/v1")
	api.Register(promRouter)

	legacyPromRouter := route.New().WithPrefix(cfg.ServerPrefix + cfg.LegacyHTTPPrefix + "/api/v1")
	api.Register(legacyPromRouter)

	//TODO(gotjosh): This custom handler is temporary until we're able to vendor the changes in:
	// https://github.com/prometheus/prometheus/pull/7125/files
	router.Path(cfg.PrometheusHTTPPrefix + "/api/v1/metadata").Handler(querier.MetadataHandler(distributor))
	router.Path(cfg.PrometheusHTTPPrefix + "/api/v1/read").Handler(querier.RemoteReadHandler(queryable))
	// A prefix is fine because external routes will be registered explicitly
	router.PathPrefix(cfg.PrometheusHTTPPrefix + "/api/v1/").Handler(promRouter)

	//TODO(gotjosh): This custom handler is temporary until we're able to vendor the changes in:
	// https://github.com/prometheus/prometheus/pull/7125/files
	router.Path(cfg.LegacyHTTPPrefix + "/api/v1/metadata").Handler(querier.MetadataHandler(distributor))
	router.Path(cfg.LegacyHTTPPrefix + "/api/v1/read").Handler(querier.RemoteReadHandler(queryable))
	// A prefix is fine because external routes will be registered explicitly
	router.PathPrefix(cfg.LegacyHTTPPrefix + "/api/v1/").Handler(legacyPromRouter)

	// Since we have a new router and the request will not go trough the default server
	// HTTP middleware stack, we need to add a middleware to extract the trace context
	// from the HTTP headers and inject it into the Go context.
	return nethttp.MiddlewareFunc(opentracing.GlobalTracer(), router.ServeHTTP, nethttp.OperationNameFunc(func(r *http.Request) string {
		return "internalQuerier"
	}))
}
