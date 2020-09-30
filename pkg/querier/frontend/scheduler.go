package frontend

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	"go.uber.org/atomic"
)

// Scheduler is responsible for queueing and dispatching queries to Queriers.
type Scheduler struct {
	log log.Logger

	limits Limits

	mtx    sync.Mutex
	cond   *sync.Cond // Notified when request is enqueued or dequeued, or querier is disconnected.
	queues *queues

	// Metrics.
	numClients    prometheus.GaugeFunc
	queueDuration prometheus.Histogram
	queueLength   *prometheus.GaugeVec

	connectedClients *atomic.Int32
}

// NewScheduler creates a new Scheduler.
func NewScheduler(cfg Config, limits Limits, log log.Logger, registerer prometheus.Registerer) (*Scheduler, error) {
	connectedClients := atomic.NewInt32(0)
	s := &Scheduler{
		log:    log,
		limits: limits,
		queues: newUserQueues(cfg.MaxOutstandingPerTenant),

		queueDuration: promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
			Namespace: "cortex",
			Name:      "query_frontend_queue_duration_seconds",
			Help:      "Time spend by requests queued.",
			Buckets:   prometheus.DefBuckets,
		}),
		numClients: promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "cortex",
			Name:      "query_frontend_connected_clients",
			Help:      "Number of worker clients currently connected to the frontend.",
		}, func() float64 { return float64(connectedClients.Load()) }),
		queueLength: promauto.With(registerer).NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "cortex",
			Name:      "query_frontend_queue_length",
			Help:      "Number of queries in the queue.",
		}, []string{"user"}),

		connectedClients: connectedClients,
	}
	s.cond = sync.NewCond(&s.mtx)
	return s, nil
}

type httpgrpcHeadersCarrier httpgrpc.HTTPRequest

func (c *httpgrpcHeadersCarrier) Set(key, val string) {
	c.Headers = append(c.Headers, &httpgrpc.Header{
		Key:    key,
		Values: []string{val},
	})
}

// Limits needed for the Query Frontend - interface used for decoupling.
type Limits interface {
	// Returns max queriers to use per tenant, or 0 if shuffle sharding is disabled.
	MaxQueriersPerUser(user string) int
}

type request struct {
	enqueueTime time.Time
	queueSpan   opentracing.Span
	originalCtx context.Context

	request  *httpgrpc.HTTPRequest
	err      chan error
	response chan *httpgrpc.HTTPResponse
}

// Enqueue a request with the scheduler.
func (s *Scheduler) Enqueue(ctx context.Context, r *EnqueueRequest) (*EnqueueResponse, error) {
	// Propagate trace context in gRPC too - this will be ignored if using HTTP.
	tracer, span := opentracing.GlobalTracer(), opentracing.SpanFromContext(ctx)
	if tracer != nil && span != nil {
		carrier := (*httpgrpcHeadersCarrier)(r.HttpRequest)
		tracer.Inject(span.Context(), opentracing.HTTPHeaders, carrier)
	}

	req := &request{
		request:     r.HttpRequest,
		originalCtx: ctx,

		// Buffer of 1 to ensure response can be written by the server side
		// of the Process stream, even if this goroutine goes away due to
		// client context cancellation.
		err:      make(chan error, 1),
		response: make(chan *httpgrpc.HTTPResponse, 1),
	}

	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	req.enqueueTime = time.Now()
	req.queueSpan, _ = opentracing.StartSpanFromContext(ctx, "queued")

	maxQueriers := s.limits.MaxQueriersPerUser(userID)

	s.mtx.Lock()
	defer s.mtx.Unlock()

	queue := s.queues.getOrAddQueue(userID, maxQueriers)
	if queue == nil {
		// This can only happen if userID is "".
		return nil, errors.New("no queue found")
	}

	select {
	case queue <- req:
		s.queueLength.WithLabelValues(userID).Inc()
		s.cond.Broadcast()
		return nil, nil
	default:
		return nil, errTooManyRequest
	}
}

// Cancel a request with the scheduler.
func (s *Scheduler) Cancel(ctx context.Context, r *CancelRequest) (*CancelResponse, error) {
	return nil, nil
}

// Process allows backends to pull requests from the frontend.
func (s *Scheduler) Process(server Frontend_ProcessServer) error {
	querierID, err := getQuerierID(server)
	if err != nil {
		return err
	}

	s.registerQuerierConnection(querierID)
	defer s.unregisterQuerierConnection(querierID)

	// If the downstream request(from querier -> frontend) is cancelled,
	// we need to ping the condition variable to unblock getNextRequestForQuerier.
	// Ideally we'd have ctx aware condition variables...
	go func() {
		<-server.Context().Done()
		s.cond.Broadcast()
	}()

	lastUserIndex := -1

	for {
		req, idx, err := s.getNextRequestForQuerier(server.Context(), lastUserIndex, querierID)
		if err != nil {
			return err
		}
		lastUserIndex = idx

		// Handle the stream sending & receiving on a goroutine so we can
		// monitoring the contexts in a select and cancel things appropriately.
		resps := make(chan *httpgrpc.HTTPResponse, 1)
		errs := make(chan error, 1)
		go func() {
			err = server.Send(&FrontendToClient{
				Type:        HTTP_REQUEST,
				HttpRequest: req.request,
			})
			if err != nil {
				errs <- err
				return
			}

			resp, err := server.Recv()
			if err != nil {
				errs <- err
				return
			}

			resps <- resp.HttpResponse
		}()

		select {
		// If the upstream request is cancelled, we need to cancel the
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

// getQueue picks a random queue and takes the next unexpired request off of it, so we
// fairly process users queries.  Will block if there are no requests.
func (s *Scheduler) getNextRequestForQuerier(ctx context.Context, lastUserIndex int, querierID string) (*request, int, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	querierWait := false

FindQueue:
	// We need to wait if there are no users, or no pending requests for given querier.
	for (s.queues.len() == 0 || querierWait) && ctx.Err() == nil {
		querierWait = false
		s.cond.Wait()
	}

	if err := ctx.Err(); err != nil {
		return nil, lastUserIndex, err
	}

	for {
		queue, userID, idx := s.queues.getNextQueueForQuerier(lastUserIndex, querierID)
		lastUserIndex = idx
		if queue == nil {
			break
		}
		/*
		  We want to dequeue the next unexpired request from the chosen tenant queue.
		  The chance of choosing a particular tenant for dequeueing is (1/active_tenants).
		  This is problematic under load, especially with other middleware enabled such as
		  querier.split-by-interval, where one request may fan out into many.
		  If expired requests aren't exhausted before checking another tenant, it would take
		  n_active_tenants * n_expired_requests_at_front_of_queue requests being processed
		  before an active request was handled for the tenant in question.
		  If this tenant meanwhile continued to queue requests,
		  it's possible that it's own queue would perpetually contain only expired requests.
		*/

		// Pick the first non-expired request from this user's queue (if any).
		for {
			lastRequest := false
			request := <-queue
			if len(queue) == 0 {
				s.queues.deleteQueue(userID)
				lastRequest = true
			}

			// Tell close() we've processed a request.
			s.cond.Broadcast()

			s.queueDuration.Observe(time.Since(request.enqueueTime).Seconds())
			s.queueLength.WithLabelValues(userID).Dec()
			request.queueSpan.Finish()

			// Ensure the request has not already expired.
			if request.originalCtx.Err() == nil {
				return request, lastUserIndex, nil
			}

			// Stop iterating on this queue if we've just consumed the last request.
			if lastRequest {
				break
			}
		}
	}

	// There are no unexpired requests, so we can get back
	// and wait for more requests.
	querierWait = true
	goto FindQueue
}

// Close the Scheduler.
func (s *Scheduler) Close() {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	for s.queues.len() > 0 {
		s.cond.Wait()
	}
}

func (s *Scheduler) registerQuerierConnection(querier string) {
	s.connectedClients.Inc()

	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.queues.addQuerierConnection(querier)
}

func (s *Scheduler) unregisterQuerierConnection(querier string) {
	s.connectedClients.Dec()

	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.queues.removeQuerierConnection(querier)
}
