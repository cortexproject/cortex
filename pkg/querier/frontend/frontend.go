package frontend

import (
	"bytes"
	"context"
	"flag"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/httpgrpc/server"
	"github.com/weaveworks/common/user"
)

var (
	queueDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "query_frontend_queue_duration_seconds",
		Help:      "Time spend by requests queued.",
		Buckets:   prometheus.DefBuckets,
	})
	retries = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "query_frontend_retries",
		Help:      "Number of times a request is retried.",
		Buckets:   []float64{0, 1, 2, 3, 4, 5},
	})
	queueLength = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "query_frontend_queue_length",
		Help:      "Number of queries in the queue.",
	})

	errServerClosing  = httpgrpc.Errorf(http.StatusTeapot, "server closing down")
	errTooManyRequest = httpgrpc.Errorf(http.StatusTooManyRequests, "too many outstanding requests")
	errCanceled       = httpgrpc.Errorf(http.StatusInternalServerError, "context cancelled")
)

// Config for a Frontend.
type Config struct {
	MaxOutstandingPerTenant int
	MaxRetries              int
	SplitQueriesByDay       bool
	AlignQueriesWithStep    bool
	CacheResults            bool
	resultsCacheConfig
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.MaxOutstandingPerTenant, "querier.max-outstanding-requests-per-tenant", 100, "Maximum number of outstanding requests per tenant per frontend; requests beyond this error with HTTP 429.")
	f.IntVar(&cfg.MaxRetries, "querier.max-retries-per-request", 5, "Maximum number of retries for a single request; beyond this, the downstream error is returned.")
	f.BoolVar(&cfg.SplitQueriesByDay, "querier.split-queries-by-day", false, "Split queries by day and execute in parallel.")
	f.BoolVar(&cfg.AlignQueriesWithStep, "querier.align-querier-with-step", false, "Mutate incoming queries to align their start and end with their step.")
	f.BoolVar(&cfg.CacheResults, "querier.cache-results", false, "Cache query results.")
	cfg.resultsCacheConfig.RegisterFlags(f)
}

// Frontend queues HTTP requests, dispatches them to backends, and handles retries
// for requests which failed.
type Frontend struct {
	cfg          Config
	log          log.Logger
	roundTripper http.RoundTripper

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
func New(cfg Config, log log.Logger) (*Frontend, error) {
	f := &Frontend{
		cfg:    cfg,
		log:    log,
		queues: map[string]chan *request{},
	}

	// Stack up the pipeline of various query range middlewares.
	queryRangeMiddleware := []queryRangeMiddleware{}
	if cfg.AlignQueriesWithStep {
		queryRangeMiddleware = append(queryRangeMiddleware, stepAlignMiddleware)
	}
	if cfg.SplitQueriesByDay {
		queryRangeMiddleware = append(queryRangeMiddleware, splitByDayMiddleware)
	}
	if cfg.CacheResults {
		queryCacheMiddleware, err := newResultsCacheMiddleware(cfg.resultsCacheConfig)
		if err != nil {
			return nil, err
		}
		queryRangeMiddleware = append(queryRangeMiddleware, instrument("results_cache"), queryCacheMiddleware)
	}

	// Finally, if the user selected any query range middleware, stitch it in.
	var roundTripper http.RoundTripper = f
	if len(queryRangeMiddleware) > 0 {
		roundTripper = &queryRangeRoundTripper{
			next: roundTripper,
			queryRangeMiddleware: merge(queryRangeMiddleware...).Wrap(&queryRangeTerminator{
				next: roundTripper,
			}),
		}
	}
	f.roundTripper = roundTripper
	f.cond = sync.NewCond(&f.mtx)
	return f, nil
}

// Close stops new requests and errors out any pending requests.
func (f *Frontend) Close() {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	for len(f.queues) > 0 {
		f.cond.Wait()
	}
}

// ServeHTTP serves HTTP requests.
func (f *Frontend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

	request := &request{
		request:     req,
		originalCtx: ctx,
		// Buffer of 1 to ensure response can be written even if client has gone away.
		err:      make(chan error, 1),
		response: make(chan *ProcessResponse, 1),
	}

	var lastErr error
	for tries := 0; tries < f.cfg.MaxRetries; tries++ {
		if err := f.queueRequest(ctx, request); err != nil {
			return nil, err
		}

		var resp *ProcessResponse
		select {
		case <-ctx.Done():
			return nil, errCanceled

		case resp = <-request.response:
		case lastErr = <-request.err:
			httpResp, ok := httpgrpc.HTTPResponseFromError(lastErr)
			if ok {
				resp = &ProcessResponse{
					HttpResponse: httpResp,
				}
			}
		}

		// Retry is we get a HTTP 500.
		if resp != nil && resp.HttpResponse.Code/100 == 5 {
			level.Error(f.log).Log("msg", "error processing request", "try", tries, "resp", resp.HttpResponse)
			continue
		}

		// Also retry for non-HTTP errors.
		if resp == nil && lastErr != nil {
			level.Error(f.log).Log("msg", "error processing request", "try", tries, "err", lastErr)
			continue
		}

		retries.Observe(float64(tries))

		return resp, nil
	}

	if lastErr != nil {
		return nil, lastErr
	}

	return nil, httpgrpc.Errorf(http.StatusInternalServerError, "Query failed after %d retries.", f.cfg.MaxRetries)
}

// Process allows backends to pull requests from the frontend.
func (f *Frontend) Process(server Frontend_ProcessServer) error {
	var (
		sendChan = make(chan *ProcessRequest)
		recvChan = make(chan *ProcessResponse, 1)

		// Need buffer of 2 so goroutines reading/writing to stream don't hang
		// around when stream dies.
		errChan = make(chan error, 2)
	)

	// If the stream from the querier is canceled, ping the condition to unblock.
	// This is done once, here (instead of in getNextRequest) as we expect calls
	// to Process to process many requests.
	go func() {
		<-server.Context().Done()
		f.cond.Broadcast()
	}()

	// Use a pair of goroutines to read/write from the stream and send to channels,
	// so we can use selects to also wait on the cancellation of the request context.
	// These goroutines will error out when the stream returns.
	go func() {
		for {
			var req *ProcessRequest
			select {
			case req = <-sendChan:
			case <-server.Context().Done():
				return
			}

			err := server.Send(req)
			if err != nil {
				errChan <- err
				return
			}
		}
	}()

	go func() {
		for {
			resp, err := server.Recv()
			if err == nil {
				recvChan <- resp
			} else {
				errChan <- err
				return
			}
		}
	}()

	for {
		request, err := f.getNextRequest(server.Context())
		if err != nil {
			return err
		}

		originalCtx := request.originalCtx

		select {
		case sendChan <- request.request:
		case err := <-errChan:
			request.err <- err
			return err
		case <-originalCtx.Done():
			return originalCtx.Err()
		}

		select {
		case resp := <-recvChan:
			request.response <- resp
		case err := <-errChan:
			request.err <- err
			return err
		case <-originalCtx.Done():
			return originalCtx.Err()
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
