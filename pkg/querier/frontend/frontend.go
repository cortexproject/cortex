package frontend

import (
	"bytes"
	"context"
	"flag"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"path"
	"sync"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/go-kit/kit/log"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/httpgrpc/server"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/chunk/cache"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

var (
	queueDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "query_frontend_queue_duration_seconds",
		Help:      "Time spend by requests queued.",
		Buckets:   prometheus.DefBuckets,
	})
	queueLength = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "query_frontend_queue_length",
		Help:      "Number of queries in the queue.",
	})
	queryRangeDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "frontend_query_range_duration_seconds",
		Help:      "Total time spent in seconds doing query range requests.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "status_code"})

	errServerClosing  = httpgrpc.Errorf(http.StatusTeapot, "server closing down")
	errTooManyRequest = httpgrpc.Errorf(http.StatusTooManyRequests, "too many outstanding requests")
	errCanceled       = httpgrpc.Errorf(http.StatusInternalServerError, "context cancelled")
)

// Config for a Frontend.
type Config struct {
	MaxOutstandingPerTenant       int  `yaml:"max_outstanding_per_tenant"`
	MaxRetries                    int  `yaml:"max_retries"`
	SplitQueriesByDay             bool `yaml:"split_queries_by_day"`
	AlignQueriesWithStep          bool `yaml:"align_queries_with_step"`
	CacheResults                  bool `yaml:"cache_results"`
	CompressResponses             bool `yaml:"compress_responses"`
	queryrange.ResultsCacheConfig `yaml:"results_cache"`
	DownstreamURL                 string `yaml:"downstream"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.MaxOutstandingPerTenant, "querier.max-outstanding-requests-per-tenant", 100, "Maximum number of outstanding requests per tenant per frontend; requests beyond this error with HTTP 429.")
	f.IntVar(&cfg.MaxRetries, "querier.max-retries-per-request", 5, "Maximum number of retries for a single request; beyond this, the downstream error is returned.")
	f.BoolVar(&cfg.SplitQueriesByDay, "querier.split-queries-by-day", false, "Split queries by day and execute in parallel.")
	f.BoolVar(&cfg.AlignQueriesWithStep, "querier.align-querier-with-step", false, "Mutate incoming queries to align their start and end with their step.")
	f.BoolVar(&cfg.CacheResults, "querier.cache-results", false, "Cache query results.")
	f.BoolVar(&cfg.CompressResponses, "querier.compress-http-responses", false, "Compress HTTP responses.")
	cfg.ResultsCacheConfig.RegisterFlags(f)
	f.StringVar(&cfg.DownstreamURL, "frontend.downstream-url", "", "URL of downstream Prometheus.")
}

// Frontend queues HTTP requests, dispatches them to backends, and handles retries
// for requests which failed.
type Frontend struct {
	cfg          Config
	log          log.Logger
	roundTripper http.RoundTripper
	cache        cache.Cache

	mtx    sync.Mutex
	cond   *sync.Cond
	queues map[string]chan *request
}

type request struct {
	enqueueTime time.Time
	queueSpan   opentracing.Span
	originalCtx context.Context

	request  *ProcessRequest
	err      chan error
	response chan *ProcessResponse
}

// New creates a new frontend.
func New(cfg Config, log log.Logger, limits *validation.Overrides) (*Frontend, error) {
	f := &Frontend{
		cfg:    cfg,
		log:    log,
		queues: map[string]chan *request{},
	}
	f.cond = sync.NewCond(&f.mtx)

	// Stack up the pipeline of various query range middlewares.
	var queryRangeMiddleware []queryrange.Middleware
	if cfg.AlignQueriesWithStep {
		queryRangeMiddleware = append(queryRangeMiddleware, queryrange.InstrumentMiddleware("step_align", queryRangeDuration), queryrange.StepAlignMiddleware)
	}
	if cfg.SplitQueriesByDay {
		queryRangeMiddleware = append(queryRangeMiddleware, queryrange.InstrumentMiddleware("split_by_day", queryRangeDuration), queryrange.SplitByDayMiddleware(limits))
	}
	if cfg.CacheResults {
		queryCacheMiddleware, cache, err := queryrange.NewResultsCacheMiddleware(log, cfg.ResultsCacheConfig, limits)
		if err != nil {
			return nil, err
		}
		f.cache = cache
		queryRangeMiddleware = append(queryRangeMiddleware, queryrange.InstrumentMiddleware("results_cache", queryRangeDuration), queryCacheMiddleware)
	}
	if cfg.MaxRetries > 0 {
		queryRangeMiddleware = append(queryRangeMiddleware, queryrange.InstrumentMiddleware("retry", queryRangeDuration), queryrange.NewRetryMiddleware(log, cfg.MaxRetries))
	}

	// If the user has specified a downstream Prometheus, then we should
	// forward requests to that.  Otherwise we will wait for queries to
	// contact us.
	var roundTripper http.RoundTripper = f
	if cfg.DownstreamURL != "" {
		u, err := url.Parse(cfg.DownstreamURL)
		if err != nil {
			return nil, err
		}

		roundTripper = RoundTripFunc(func(r *http.Request) (*http.Response, error) {
			r.URL.Scheme = u.Scheme
			r.URL.Host = u.Host
			r.URL.Path = path.Join(u.Path, r.URL.Path)
			return http.DefaultTransport.RoundTrip(r)
		})
	}

	// Finally, if the user selected any query range middleware, stitch it in.
	if len(queryRangeMiddleware) > 0 {
		roundTripper = queryrange.NewRoundTripper(
			roundTripper,
			queryrange.MergeMiddlewares(queryRangeMiddleware...).Wrap(&queryrange.ToRoundTripperMiddleware{Next: roundTripper}),
			limits,
		)
	}
	f.roundTripper = roundTripper
	return f, nil
}

// RoundTripFunc is to http.RoundTripper what http.HandlerFunc is to http.Handler.
type RoundTripFunc func(*http.Request) (*http.Response, error)

// RoundTrip implements http.RoundTripper.
func (f RoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

// Close stops new requests and errors out any pending requests.
func (f *Frontend) Close() {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	for len(f.queues) > 0 {
		f.cond.Wait()
	}
	if f.cache != nil {
		f.cache.Stop()
		f.cache = nil
	}
}

// Handler for HTTP requests.
func (f *Frontend) Handler() http.Handler {
	if f.cfg.CompressResponses {
		return gziphandler.GzipHandler(http.HandlerFunc(f.handle))
	}
	return http.HandlerFunc(f.handle)
}

func (f *Frontend) handle(w http.ResponseWriter, r *http.Request) {
	resp, err := f.roundTripper.RoundTrip(r)
	if err != nil {
		server.WriteError(w, err)
		return
	}

	hs := w.Header()
	for h, vs := range resp.Header {
		hs[h] = vs
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// RoundTrip implement http.Transport.
func (f *Frontend) RoundTrip(r *http.Request) (*http.Response, error) {
	req, err := server.HTTPRequest(r)
	if err != nil {
		return nil, err
	}

	resp, err := f.RoundTripGRPC(r.Context(), &ProcessRequest{
		HttpRequest: req,
	})
	if err != nil {
		return nil, err
	}

	httpResp := &http.Response{
		StatusCode: int(resp.HttpResponse.Code),
		Body:       ioutil.NopCloser(bytes.NewReader(resp.HttpResponse.Body)),
		Header:     http.Header{},
	}
	for _, h := range resp.HttpResponse.Headers {
		httpResp.Header[h.Key] = h.Values
	}
	return httpResp, nil
}

type httpgrpcHeadersCarrier httpgrpc.HTTPRequest

func (c *httpgrpcHeadersCarrier) Set(key, val string) {
	c.Headers = append(c.Headers, &httpgrpc.Header{
		Key:    key,
		Values: []string{val},
	})
}

// RoundTripGRPC round trips a proto (instread of a HTTP request).
func (f *Frontend) RoundTripGRPC(ctx context.Context, req *ProcessRequest) (*ProcessResponse, error) {
	// Propagate trace context in gRPC too - this will be ignored if using HTTP.
	tracer, span := opentracing.GlobalTracer(), opentracing.SpanFromContext(ctx)
	if tracer != nil && span != nil {
		carrier := (*httpgrpcHeadersCarrier)(req.HttpRequest)
		tracer.Inject(span.Context(), opentracing.HTTPHeaders, carrier)
	}

	request := request{
		request:     req,
		originalCtx: ctx,

		// Buffer of 1 to ensure response can be written by the server side
		// of the Process stream, even if this goroutine goes away due to
		// client context cancellation.
		err:      make(chan error, 1),
		response: make(chan *ProcessResponse, 1),
	}

	if err := f.queueRequest(ctx, &request); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, errCanceled

	case resp := <-request.response:
		return resp, nil

	case err := <-request.err:
		return nil, err
	}
}

// Process allows backends to pull requests from the frontend.
func (f *Frontend) Process(server Frontend_ProcessServer) error {
	// If the downstream request(from querier -> frontend) is cancelled,
	// we need to ping the condition variable to unblock getNextRequest.
	// Ideally we'd have ctx aware condition variables...
	go func() {
		<-server.Context().Done()
		f.cond.Broadcast()
	}()

	for {
		req, err := f.getNextRequest(server.Context())
		if err != nil {
			return err
		}

		// Handle the stream sending & receiving on a goroutine so we can
		// monitoring the contexts in a select and cancel things appropriately.
		resps := make(chan *ProcessResponse, 1)
		errs := make(chan error, 1)
		go func() {
			err = server.Send(req.request)
			if err != nil {
				errs <- err
				return
			}

			resp, err := server.Recv()
			if err != nil {
				errs <- err
				return
			}

			resps <- resp
		}()

		select {
		// If the upstream reqeust is cancelled, we need to cancel the
		// downstream req.  Only way we can do that is to close the stream.
		// The worker client is expecting this semantics.
		case <-req.originalCtx.Done():
			return req.originalCtx.Err()

		// Is there was an error handling this request due to network IO,
		// then error out this upstream request _and_ stream.
		case err := <-errs:
			req.err <- err
			return err

		// Happy path: propagate the response.
		case resp := <-resps:
			req.response <- resp
		}
	}
}

func (f *Frontend) queueRequest(ctx context.Context, req *request) error {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return err
	}

	req.enqueueTime = time.Now()
	req.queueSpan, _ = opentracing.StartSpanFromContext(ctx, "queued")

	f.mtx.Lock()
	defer f.mtx.Unlock()

	queue, ok := f.queues[userID]
	if !ok {
		queue = make(chan *request, f.cfg.MaxOutstandingPerTenant)
		f.queues[userID] = queue
	}

	select {
	case queue <- req:
		queueLength.Add(1)
		f.cond.Broadcast()
		return nil
	default:
		return errTooManyRequest
	}
}

// getQueue picks a random queue and takes the next request off of it, so we
// fairly process users queries.  Will block if there are no requests.
func (f *Frontend) getNextRequest(ctx context.Context) (*request, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	for len(f.queues) == 0 && ctx.Err() == nil {
		f.cond.Wait()
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	i, n := 0, rand.Intn(len(f.queues))
	for userID, queue := range f.queues {
		if i < n {
			i++
			continue
		}

		request := <-queue
		if len(queue) == 0 {
			delete(f.queues, userID)
		}

		// Tell close() we've processed a request.
		f.cond.Broadcast()

		queueDuration.Observe(time.Now().Sub(request.enqueueTime).Seconds())
		queueLength.Add(-1)
		request.queueSpan.Finish()

		return request, nil
	}

	panic("should never happen")
}
