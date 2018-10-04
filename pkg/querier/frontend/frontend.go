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
	"github.com/opentracing-contrib/go-stdlib/nethttp"
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
	request     *httpgrpc.HTTPRequest
	err         chan error
	response    chan *httpgrpc.HTTPResponse
}

// New creates a new frontend.
func New(cfg Config, log log.Logger) (*Frontend, error) {
	f := &Frontend{
		cfg:    cfg,
		log:    log,
		queues: map[string]chan *request{},
	}

	// We need to do the opentracing at the leafs of the roundtrippers, as a
	// single request could turn into multiple requests.
	tracingRoundTripper := &nethttp.Transport{
		RoundTripper: f,
	}
	var roundTripper http.RoundTripper = RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		req, ht := nethttp.TraceRequest(opentracing.GlobalTracer(), req)
		defer ht.Finish()

		return tracingRoundTripper.RoundTrip(req)
	})

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
		queryRangeMiddleware = append(queryRangeMiddleware, queryCacheMiddleware)
	}

	// Finally, if the user selected any query range middleware, stitch it in.
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
	ctx := r.Context()
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	req, err := server.HTTPRequest(r)
	if err != nil {
		return nil, err
	}

	request := &request{
		request: req,
		// Buffer of 1 to ensure response can be written even if client has gone away.
		err:      make(chan error, 1),
		response: make(chan *httpgrpc.HTTPResponse, 1),
	}

	var lastErr error
	for tries := 0; tries < f.cfg.MaxRetries; tries++ {
		if err := f.queueRequest(userID, request); err != nil {
			return nil, err
		}

		var resp *httpgrpc.HTTPResponse
		select {
		case <-ctx.Done():
			// TODO propagate cancellation.
			//request.Cancel()
			return nil, errCanceled

		case resp = <-request.response:
		case lastErr = <-request.err:
			resp, _ = httpgrpc.HTTPResponseFromError(lastErr)
		}

		// Only retry is we get a HTTP 500 or non-HTTP error.
		if resp == nil || resp.Code/100 == 5 {
			level.Error(f.log).Log("msg", "error processing request", "try", tries, "err", lastErr, "resp", resp)
			lastErr = httpgrpc.ErrorFromHTTPResponse(resp)
			continue
		}

		retries.Observe(float64(tries))

		httpResp := &http.Response{
			StatusCode: int(resp.Code),
			Body:       ioutil.NopCloser(bytes.NewReader(resp.Body)),
			Header:     http.Header{},
		}
		for _, h := range resp.Headers {
			httpResp.Header[h.Key] = h.Values
		}
		return httpResp, nil
	}

	return nil, httpgrpc.Errorf(http.StatusInternalServerError, "Query failed after %d retries, err: %s", f.cfg.MaxRetries, lastErr)
}

// Process allows backends to pull requests from the frontend.
func (f *Frontend) Process(server Frontend_ProcessServer) error {

	// If this request is canceled, ping the condition to unblock. This is done
	// once, here (instead of in getNextRequest) as we expect calls to Process to
	// process many requests.
	go func() {
		<-server.Context().Done()
		f.cond.Broadcast()
	}()

	for {
		request, err := f.getNextRequest(server.Context())
		if err != nil {
			return err
		}

		if err := server.Send(&ProcessRequest{
			HttpRequest: request.request,
		}); err != nil {
			request.err <- err
			return err
		}

		response, err := server.Recv()
		if err != nil {
			request.err <- err
			return err
		}

		request.response <- response.HttpResponse
	}
}

func (f *Frontend) queueRequest(userID string, req *request) error {
	req.enqueueTime = time.Now()

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
		return request, nil
	}

	panic("should never happen")
}
