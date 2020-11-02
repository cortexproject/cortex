package frontend2

import (
	"context"
	"errors"
	"flag"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/httpgrpc"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/util/services"
)

var (
	errTooManyRequests       = errors.New("too many outstanding requests")
	errSchedulerIsNotRunning = errors.New("scheduler is not running")
)

// Scheduler is responsible for queueing and dispatching queries to Queriers.
type Scheduler struct {
	services.Service

	log log.Logger

	limits Limits

	connectedFrontendsMu sync.Mutex
	connectedFrontends   map[string]*connectedFrontend

	connectedQuerierWorkers *atomic.Int32

	mtx             sync.Mutex
	cond            *sync.Cond // Notified when request is enqueued or dequeued, or querier is disconnected.
	queues          *queues
	pendingRequests map[requestKey]*schedulerRequest // Request is kept in this map even after being dispatched to querier. It can still be canceled at that time.

	// Metrics.
	connectedQuerierClients  prometheus.GaugeFunc
	connectedFrontendClients prometheus.GaugeFunc
	queueDuration            prometheus.Histogram
	queueLength              *prometheus.GaugeVec
}

type requestKey struct {
	frontendAddr string
	queryID      uint64
}

type connectedFrontend struct {
	connections int

	// This context is used for running all queries from the same frontend.
	// When last frontend connection is closed, context is canceled.
	ctx    context.Context
	cancel context.CancelFunc
}

type SchedulerConfig struct {
	MaxOutstandingPerTenant int `yaml:"max_outstanding_requests_per_tenant"`
}

func (cfg *SchedulerConfig) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.MaxOutstandingPerTenant, "query-scheduler.max-outstanding-requests-per-tenant", 100, "Maximum number of outstanding requests per tenant per query-scheduler. In-flight requests above this limit will fail with HTTP response status code 429.")
}

// NewScheduler creates a new Scheduler.
func NewScheduler(cfg SchedulerConfig, limits Limits, log log.Logger, registerer prometheus.Registerer) (*Scheduler, error) {
	s := &Scheduler{
		log:    log,
		limits: limits,

		queues:                  newUserQueues(cfg.MaxOutstandingPerTenant),
		pendingRequests:         map[requestKey]*schedulerRequest{},
		connectedFrontends:      map[string]*connectedFrontend{},
		connectedQuerierWorkers: atomic.NewInt32(0),
	}
	s.cond = sync.NewCond(&s.mtx)

	s.queueDuration = promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
		Name:    "cortex_query_scheduler_queue_duration_seconds",
		Help:    "Time spend by requests in queue before getting picked up by a querier.",
		Buckets: prometheus.DefBuckets,
	})
	s.connectedQuerierClients = promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_query_scheduler_connected_querier_clients",
		Help: "Number of querier worker clients currently connected to the query-scheduler.",
	}, s.getConnectedQuerierClientsMetric)
	s.connectedFrontendClients = promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_query_scheduler_connected_frontend_clients",
		Help: "Number of query-frontend worker clients currently connected to the query-scheduler.",
	}, s.getConnectedFrontendClientsMetric)
	s.queueLength = promauto.With(registerer).NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_query_scheduler_queue_length",
		Help: "Number of queries in the queue.",
	}, []string{"user"})

	s.Service = services.NewIdleService(nil, s.stopping)
	return s, nil
}

// Used to transfer trace information from/to HTTP request.
type httpgrpcHeadersCarrier httpgrpc.HTTPRequest

func (c *httpgrpcHeadersCarrier) Set(key, val string) {
	c.Headers = append(c.Headers, &httpgrpc.Header{
		Key:    key,
		Values: []string{val},
	})
}

func (c *httpgrpcHeadersCarrier) ForeachKey(handler func(key, val string) error) error {
	for _, h := range c.Headers {
		for _, v := range h.Values {
			if err := handler(h.Key, v); err != nil {
				return err
			}
		}
	}
	return nil
}

func getParentSpanForRequest(tracer opentracing.Tracer, req *httpgrpc.HTTPRequest) (opentracing.SpanContext, error) {
	if tracer == nil {
		return nil, nil
	}

	carrier := (*httpgrpcHeadersCarrier)(req)
	extracted, err := tracer.Extract(opentracing.HTTPHeaders, carrier)
	if err == opentracing.ErrSpanContextNotFound {
		err = nil
	}
	return extracted, err
}

// Limits needed for the Query Frontend - interface used for decoupling.
type Limits interface {
	// Returns max queriers to use per tenant, or 0 if shuffle sharding is disabled.
	MaxQueriersPerUser(user string) int
}

type schedulerRequest struct {
	frontendAddress string
	userID          string
	queryID         uint64
	request         *httpgrpc.HTTPRequest

	enqueueTime time.Time

	ctx       context.Context
	ctxCancel context.CancelFunc
	queueSpan opentracing.Span

	// This is only used for testing.
	parentSpanContext opentracing.SpanContext
}

// This method handles connection from frontend.
func (s *Scheduler) FrontendLoop(frontend SchedulerForFrontend_FrontendLoopServer) error {
	frontendAddress, frontendCtx, err := s.frontendConnected(frontend)
	if err != nil {
		return err
	}
	defer s.frontendDisconnected(frontendAddress)

	// Response to INIT. If scheduler is not running, we skip for-loop, send SHUTTING_DOWN and exit this method.
	if s.State() == services.Running {
		if err := frontend.Send(&SchedulerToFrontend{Status: OK}); err != nil {
			return err
		}
	}

	// We stop accepting new queries in Stopping state. By returning quickly, we disconnect frontends, which in turns
	// cancels all their queries.
	for s.State() == services.Running {
		msg, err := frontend.Recv()
		if err != nil {
			return err
		}

		if s.State() != services.Running {
			break // break out of the loop, and send SHUTTING_DOWN message.
		}

		var resp *SchedulerToFrontend

		switch msg.GetType() {
		case ENQUEUE:
			err = s.enqueueRequest(frontendCtx, frontendAddress, msg)
			switch {
			case err == nil:
				resp = &SchedulerToFrontend{Status: OK}
			case err == errTooManyRequests:
				resp = &SchedulerToFrontend{Status: TOO_MANY_REQUESTS_PER_TENANT}
			default:
				resp = &SchedulerToFrontend{Status: ERROR, Error: err.Error()}
			}

		case CANCEL:
			s.cancelRequest(frontendAddress, msg.QueryID)
			resp = &SchedulerToFrontend{Status: OK}

		default:
			level.Error(s.log).Log("msg", "unknown request type from frontend", "addr", frontendAddress, "type", msg.GetType())
			return errors.New("unknown request type")
		}

		err = frontend.Send(resp)
		// Failure to send response results in ending this connection.
		if err != nil {
			return err
		}
	}

	// Report shutdown back to frontend, so that it can retry with different scheduler. Also stop the frontend loop.
	return frontend.Send(&SchedulerToFrontend{Status: SHUTTING_DOWN})
}

func (s *Scheduler) frontendConnected(frontend SchedulerForFrontend_FrontendLoopServer) (string, context.Context, error) {
	msg, err := frontend.Recv()
	if err != nil {
		return "", nil, err
	}
	if msg.Type != INIT || msg.FrontendAddress == "" {
		return "", nil, errors.New("no frontend address")
	}

	s.connectedFrontendsMu.Lock()
	defer s.connectedFrontendsMu.Unlock()

	cf := s.connectedFrontends[msg.FrontendAddress]
	if cf == nil {
		cf = &connectedFrontend{
			connections: 0,
		}
		cf.ctx, cf.cancel = context.WithCancel(context.Background())
		s.connectedFrontends[msg.FrontendAddress] = cf
	}

	cf.connections++
	return msg.FrontendAddress, cf.ctx, nil
}

func (s *Scheduler) frontendDisconnected(frontendAddress string) {
	s.connectedFrontendsMu.Lock()
	defer s.connectedFrontendsMu.Unlock()

	cf := s.connectedFrontends[frontendAddress]
	cf.connections--
	if cf.connections == 0 {
		delete(s.connectedFrontends, frontendAddress)
		cf.cancel()
	}
}

func (s *Scheduler) enqueueRequest(frontendContext context.Context, frontendAddr string, msg *FrontendToScheduler) error {
	// Create new context for this request, to support cancellation.
	ctx, cancel := context.WithCancel(frontendContext)
	shouldCancel := true
	defer func() {
		if shouldCancel {
			cancel()
		}
	}()

	// Extract tracing information from headers in HTTP request. FrontendContext doesn't have the correct tracing
	// information, since that is a long-running request.
	tracer := opentracing.GlobalTracer()
	parentSpanContext, err := getParentSpanForRequest(tracer, msg.HttpRequest)
	if err != nil {
		return err
	}

	userID := msg.GetUserID()

	req := &schedulerRequest{
		frontendAddress: frontendAddr,
		userID:          msg.UserID,
		queryID:         msg.QueryID,
		request:         msg.HttpRequest,
	}

	req.parentSpanContext = parentSpanContext
	req.queueSpan, req.ctx = opentracing.StartSpanFromContextWithTracer(ctx, tracer, "queued", opentracing.ChildOf(parentSpanContext))
	req.enqueueTime = time.Now()
	req.ctxCancel = cancel

	maxQueriers := s.limits.MaxQueriersPerUser(userID)

	s.mtx.Lock()
	defer s.mtx.Unlock()

	queue := s.queues.getOrAddQueue(userID, maxQueriers)
	if queue == nil {
		// This can only happen if userID is "".
		return errors.New("no queue found")
	}

	select {
	case queue <- req:
		shouldCancel = false
		s.pendingRequests[requestKey{frontendAddr: frontendAddr, queryID: msg.QueryID}] = req
		s.queueLength.WithLabelValues(userID).Inc()
		s.cond.Broadcast()
		return nil
	default:
		return errTooManyRequests
	}
}

// This method doesn't do removal from the queue. That will be handled later by getNextRequestForQuerier when it finds
// this request with canceled context.
func (s *Scheduler) cancelRequest(frontendAddr string, queryID uint64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	key := requestKey{frontendAddr: frontendAddr, queryID: queryID}
	req := s.pendingRequests[key]
	if req != nil {
		req.ctxCancel()
	}
	delete(s.pendingRequests, key)
}

// QuerierLoop is started by querier to receive queries from scheduler.
func (s *Scheduler) QuerierLoop(querier SchedulerForQuerier_QuerierLoopServer) error {
	resp, err := querier.Recv()
	if err != nil {
		return err
	}

	querierID := resp.GetQuerierID()

	s.registerQuerierConnection(querierID)
	defer s.unregisterQuerierConnection(querierID)

	// If the downstream connection to querier is cancelled,
	// we need to ping the condition variable to unblock getNextRequestForQuerier.
	// Ideally we'd have ctx aware condition variables...
	go func() {
		<-querier.Context().Done()
		s.cond.Broadcast()
	}()

	lastUserIndex := -1

	// In stopping state scheduler is not accepting new queries, but still dispatching queries in the queues.
	for s.isRunningOrStopping() {
		req, idx, err := s.getNextRequestForQuerier(querier.Context(), lastUserIndex, querierID)
		if err != nil {
			return err
		}
		lastUserIndex = idx

		if err := s.forwardRequestToQuerier(querier, req); err != nil {
			return err
		}
	}

	return errSchedulerIsNotRunning
}

func (s *Scheduler) forwardRequestToQuerier(querier SchedulerForQuerier_QuerierLoopServer, req *schedulerRequest) error {
	// Make sure to cancel request at the end to cleanup resources.
	defer s.cancelRequest(req.frontendAddress, req.queryID)

	// Handle the stream sending & receiving on a goroutine so we can
	// monitoring the contexts in a select and cancel things appropriately.
	errCh := make(chan error, 1)
	go func() {
		err := querier.Send(&SchedulerToQuerier{
			UserID:          req.userID,
			QueryID:         req.queryID,
			FrontendAddress: req.frontendAddress,
			HttpRequest:     req.request,
		})
		if err != nil {
			errCh <- err
			return
		}

		_, err = querier.Recv()
		errCh <- err
	}()

	select {
	case <-req.ctx.Done():
		// If the upstream request is cancelled (eg. frontend issued CANCEL or closed connection),
		// we need to cancel the downstream req. Only way we can do that is to close the stream (by returning error here).
		// Querier is expecting this semantics.
		return req.ctx.Err()

	case err := <-errCh:
		// Is there was an error handling this request due to network IO,
		// then error out this upstream request _and_ stream.
		// TODO: if err is not nil, scheduler should notify frontend using the frontend address.
		return err
	}
}

// getQueue picks a random queue and takes the next unexpired request off of it, so we
// fairly process users queries.  Will block if there are no requests.
func (s *Scheduler) getNextRequestForQuerier(ctx context.Context, lastUserIndex int, querierID string) (*schedulerRequest, int, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	querierWait := false

FindQueue:
	// We need to wait if there are no users, or no pending requests for given querier.
	for (s.queues.len() == 0 || querierWait) && ctx.Err() == nil && s.isRunningOrStopping() {
		querierWait = false
		s.cond.Wait()
	}

	if err := ctx.Err(); err != nil {
		return nil, lastUserIndex, err
	}

	if !s.isRunningOrStopping() {
		return nil, lastUserIndex, errSchedulerIsNotRunning
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
			if request.ctx.Err() == nil {
				return request, lastUserIndex, nil
			}

			// Make sure cancel is called for all requests.
			request.ctxCancel()
			delete(s.pendingRequests, requestKey{request.frontendAddress, request.queryID})

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

func (s *Scheduler) isRunningOrStopping() bool {
	st := s.State()
	return st == services.Running || st == services.Stopping
}

// Close the Scheduler.
func (s *Scheduler) stopping(_ error) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for s.queues.len() > 0 && s.connectedQuerierWorkers.Load() > 0 {
		s.cond.Wait()
	}

	// If there are still queriers waiting for requests, they get notified.
	// (They would also be notified if gRPC server shuts down).
	s.cond.Broadcast()
	return nil
}

func (s *Scheduler) registerQuerierConnection(querier string) {
	s.connectedQuerierWorkers.Inc()

	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.queues.addQuerierConnection(querier)
}

func (s *Scheduler) unregisterQuerierConnection(querier string) {
	s.connectedQuerierWorkers.Dec()

	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.queues.removeQuerierConnection(querier)
}

func (s *Scheduler) getConnectedQuerierClientsMetric() float64 {
	return float64(s.connectedQuerierWorkers.Load())
}

func (s *Scheduler) getConnectedFrontendClientsMetric() float64 {
	s.connectedFrontendsMu.Lock()
	defer s.connectedFrontendsMu.Unlock()

	count := 0
	for _, workers := range s.connectedFrontends {
		count += workers.connections
	}

	return float64(count)
}
