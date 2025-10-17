package scheduler

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/distributed_execution"
	"github.com/cortexproject/cortex/pkg/distributed_execution/plan_fragments"
	"github.com/cortexproject/cortex/pkg/frontend/v2/frontendv2pb"
	"github.com/cortexproject/cortex/pkg/querier/stats" //lint:ignore faillint scheduler needs to retrieve priority from the context
	"github.com/cortexproject/cortex/pkg/scheduler/fragment_table"
	"github.com/cortexproject/cortex/pkg/scheduler/queue"
	"github.com/cortexproject/cortex/pkg/scheduler/schedulerpb"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/grpcclient"
	"github.com/cortexproject/cortex/pkg/util/httpgrpcutil"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

var (
	errSchedulerIsNotRunning = errors.New("scheduler is not running")
)

// Scheduler is responsible for queueing and dispatching queries to Queriers.
type Scheduler struct {
	services.Service

	cfg Config
	log log.Logger

	limits Limits

	connectedFrontendsMu sync.Mutex
	connectedFrontends   map[string]*connectedFrontend

	requestQueue *queue.RequestQueue
	activeUsers  *util.ActiveUsersCleanupService

	pendingRequestsMu sync.Mutex

	pendingRequests map[requestKey]*schedulerRequest // Request is kept in this map even after being dispatched to querier. It can still be canceled at that time.

	// Subservices manager.
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher

	// Metrics.
	queueLength              *prometheus.GaugeVec
	discardedRequests        *prometheus.CounterVec
	connectedQuerierClients  prometheus.GaugeFunc
	connectedFrontendClients prometheus.GaugeFunc
	queueDuration            prometheus.Histogram

	// Enables or disables distributed query execution functionality
	distributedExecEnabled bool
	fragmenter             plan_fragments.Fragmenter     // Splits logical plans into executable fragments
	fragmentTable          *fragment_table.FragmentTable // Tracks fragment execution state and querier assignments

	// Maps queries to their fragment IDs for efficient query cancellation.
	// Using this map avoids the need to scan all pending requests to find
	// fragments belonging to a specific query.
	queryFragmentRegistry map[queryKey][]uint64
}

type queryKey struct {
	frontendAddr string
	queryID      uint64
}
type requestKey struct {
	queryKey   queryKey
	fragmentID uint64
}

type connectedFrontend struct {
	connections int

	// This context is used for running all queries from the same frontend.
	// When last frontend connection is closed, context is canceled.
	ctx    context.Context
	cancel context.CancelFunc
}

type Config struct {
	QuerierForgetDelay time.Duration     `yaml:"querier_forget_delay"`
	GRPCClientConfig   grpcclient.Config `yaml:"grpc_client_config" doc:"description=This configures the gRPC client used to report errors back to the query-frontend."`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	flagext.DeprecatedFlag(f, "query-scheduler.max-outstanding-requests-per-tenant", "Deprecated: Use frontend.max-outstanding-requests-per-tenant instead.", util_log.Logger)
	f.DurationVar(&cfg.QuerierForgetDelay, "query-scheduler.querier-forget-delay", 0, "If a querier disconnects without sending notification about graceful shutdown, the query-scheduler will keep the querier in the tenant's shard until the forget delay has passed. This feature is useful to reduce the blast radius when shuffle-sharding is enabled.")
	cfg.GRPCClientConfig.RegisterFlagsWithPrefix("query-scheduler.grpc-client-config", "", f)
}

// NewScheduler creates a new Scheduler.
func NewScheduler(cfg Config, limits Limits, log log.Logger, registerer prometheus.Registerer, distributedExecEnabled bool) (*Scheduler, error) {
	s := &Scheduler{
		cfg:    cfg,
		log:    log,
		limits: limits,

		pendingRequests:    map[requestKey]*schedulerRequest{},
		connectedFrontends: map[string]*connectedFrontend{},

		fragmentTable:          fragment_table.NewFragmentTable(2 * time.Minute),
		fragmenter:             plan_fragments.NewPlanFragmenter(),
		distributedExecEnabled: distributedExecEnabled,
		queryFragmentRegistry:  map[queryKey][]uint64{},
	}

	s.queueLength = promauto.With(registerer).NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_query_scheduler_queue_length",
		Help: "Number of queries in the queue.",
	}, []string{"user", "priority", "type"})

	s.discardedRequests = promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_query_scheduler_discarded_requests_total",
		Help: "Total number of query requests discarded.",
	}, []string{"user", "priority"})

	s.requestQueue = queue.NewRequestQueue(cfg.QuerierForgetDelay, s.queueLength, s.discardedRequests, s.limits, registerer)

	s.queueDuration = promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
		Name:    "cortex_query_scheduler_queue_duration_seconds",
		Help:    "Time spend by requests in queue before getting picked up by a querier.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30, 60},
	})
	s.connectedQuerierClients = promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_query_scheduler_connected_querier_clients",
		Help: "Number of querier worker clients currently connected to the query-scheduler.",
	}, s.requestQueue.GetConnectedQuerierWorkersMetric)
	s.connectedFrontendClients = promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_query_scheduler_connected_frontend_clients",
		Help: "Number of query-frontend worker clients currently connected to the query-scheduler.",
	}, s.getConnectedFrontendClientsMetric)

	s.activeUsers = util.NewActiveUsersCleanupWithDefaultValues(s.cleanupMetricsForInactiveUser)

	var err error
	s.subservices, err = services.NewManager(s.requestQueue, s.activeUsers)
	if err != nil {
		return nil, err
	}

	s.Service = services.NewBasicService(s.starting, s.running, s.stopping)
	return s, nil
}

// Limits needed for the Query Scheduler - interface used for decoupling.
type Limits interface {
	// MaxQueriersPerUser returns max queriers to use per tenant, or 0 if shuffle sharding is disabled.
	MaxQueriersPerUser(user string) float64

	queue.Limits
}

type schedulerRequest struct {
	frontendAddress string
	userID          string
	queryID         uint64
	request         *httpgrpc.HTTPRequest
	statsEnabled    bool

	enqueueTime time.Time

	ctx       context.Context
	ctxCancel context.CancelFunc
	queueSpan opentracing.Span

	// This is only used for testing.
	parentSpanContext opentracing.SpanContext

	// fragment represents a portion of the query plan.
	// In distributed execution mode, contains a specific plan segment.
	// In non-distributed mode, only marks the query as root fragment.
	fragment plan_fragments.Fragment
}

func (s schedulerRequest) Priority() int64 {
	priority, ok := stats.FromContext(s.ctx).LoadPriority()
	if !ok {
		return 0
	}

	return priority
}

func getPlanFromHTTPRequest(req *httpgrpc.HTTPRequest) ([]byte, error) {
	if req.Body == nil {
		return nil, nil
	}
	values, err := url.ParseQuery(string(req.Body))
	if err != nil {
		return nil, err
	}
	plan := values.Get("plan")
	return []byte(plan), nil
}

// FrontendLoop handles connection from frontend.
func (s *Scheduler) FrontendLoop(frontend schedulerpb.SchedulerForFrontend_FrontendLoopServer) error {
	frontendAddress, frontendCtx, err := s.frontendConnected(frontend)
	if err != nil {
		return err
	}
	defer s.frontendDisconnected(frontendAddress)

	// Response to INIT. If scheduler is not running, we skip for-loop, send SHUTTING_DOWN and exit this method.
	if s.State() == services.Running {
		if err := frontend.Send(&schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}); err != nil {
			return err
		}
	}

	// We stop accepting new queries in Stopping state. By returning quickly, we disconnect frontends, which in turns
	// cancels all their queries.
	for s.State() == services.Running {
		msg, err := frontend.Recv()
		if err != nil {
			// No need to report this as error, it is expected when query-frontend performs SendClose() (as frontendSchedulerWorker does).
			if err == io.EOF {
				return nil
			}
			return err
		}

		if s.State() != services.Running {
			break // break out of the loop, and send SHUTTING_DOWN message.
		}

		var resp *schedulerpb.SchedulerToFrontend

		switch msg.GetType() {
		case schedulerpb.ENQUEUE:

			// If there is a logical plan in the request body, we will fragment it before enqueueing
			// otherwise, it will be a single request and is the root and can be enqueued directly
			byteLP, err := getPlanFromHTTPRequest(msg.HttpRequest)
			if err != nil {
				return err
			}
			if len(byteLP) != 0 {
				err = s.fragmentAndEnqueueRequest(frontendCtx, frontendAddress, msg, byteLP)
			} else {
				err = s.enqueueRequest(frontendCtx, frontendAddress, msg, plan_fragments.Fragment{FragmentID: 0, IsRoot: true})
			}

			switch err {
			case nil:
				resp = &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
			case queue.ErrTooManyRequests:
				resp = &schedulerpb.SchedulerToFrontend{Status: schedulerpb.TOO_MANY_REQUESTS_PER_TENANT}
			default:
				resp = &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: err.Error()}
			}

		case schedulerpb.CANCEL:
			s.cancelRequestAndRemoveFromPending(frontendAddress, msg.QueryID, 0, true)
			resp = &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}

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
	return frontend.Send(&schedulerpb.SchedulerToFrontend{Status: schedulerpb.SHUTTING_DOWN})
}

func (s *Scheduler) frontendConnected(frontend schedulerpb.SchedulerForFrontend_FrontendLoopServer) (string, context.Context, error) {
	msg, err := frontend.Recv()
	if err != nil {
		return "", nil, err
	}
	if msg.Type != schedulerpb.INIT || msg.FrontendAddress == "" {
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

func updatePlanInHTTPRequest(fragment plan_fragments.Fragment) ([]byte, error) {
	byteLP, err := logicalplan.Marshal(fragment.Node)
	if err != nil {
		return nil, err
	}
	form := url.Values{}
	form.Add("plan", string(byteLP))
	return []byte(form.Encode()), nil
}

func (s *Scheduler) fragmentAndEnqueueRequest(frontendContext context.Context, frontendAddr string, msg *schedulerpb.FrontendToScheduler, byteLogicalPlan []byte) error {

	// un-serialize logical plan and fragment it
	lpNode, err := distributed_execution.Unmarshal(byteLogicalPlan)
	if err != nil {
		return err
	}

	fragments, err := s.fragmenter.Fragment(msg.QueryID, lpNode)
	if err != nil {
		return err
	}

	for _, fragment := range fragments {
		frag := fragment
		if err := func() error {
			// update http request body with the new fragmented logical plan
			newBody, err := updatePlanInHTTPRequest(frag)
			if err != nil {
				return err
			}
			msg.HttpRequest = &httpgrpc.HTTPRequest{
				Method:  msg.HttpRequest.Method,
				Url:     msg.HttpRequest.Url,
				Headers: msg.HttpRequest.Headers,
				Body:    newBody,
			}

			err = s.enqueueRequest(frontendContext, frontendAddr, msg, frag)

			// if there is an error in any of the process enqueueing the fragments
			// immediately propagate the error back
			return err
		}(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Scheduler) enqueueRequest(frontendContext context.Context, frontendAddr string, msg *schedulerpb.FrontendToScheduler, fragment plan_fragments.Fragment) error {
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
	parentSpanContext, err := httpgrpcutil.GetParentSpanForRequest(tracer, msg.HttpRequest)
	if err != nil {
		return err
	}

	userID := msg.GetUserID()

	req := &schedulerRequest{
		frontendAddress: frontendAddr,
		userID:          msg.UserID,
		queryID:         msg.QueryID,
		request:         msg.HttpRequest,
		statsEnabled:    msg.StatsEnabled,
		fragment:        fragment,
	}

	now := time.Now()

	req.parentSpanContext = parentSpanContext
	req.queueSpan, req.ctx = opentracing.StartSpanFromContextWithTracer(ctx, tracer, "queued", opentracing.ChildOf(parentSpanContext))
	req.enqueueTime = now
	req.ctxCancel = cancel

	// aggregate the max queriers limit in the case of a multi tenant query
	tenantIDs, err := tenant.TenantIDsFromOrgID(userID)
	if err != nil {
		return err
	}
	maxQueriers := validation.SmallestPositiveNonZeroFloat64PerTenant(tenantIDs, s.limits.MaxQueriersPerUser)

	s.activeUsers.UpdateUserTimestamp(userID, now)
	return s.requestQueue.EnqueueRequest(userID, req, maxQueriers, func() {
		shouldCancel = false

		s.pendingRequestsMu.Lock()
		defer s.pendingRequestsMu.Unlock()

		queryKey := queryKey{frontendAddr: frontendAddr, queryID: msg.QueryID}
		s.queryFragmentRegistry[queryKey] = append(s.queryFragmentRegistry[queryKey], req.fragment.FragmentID)
		s.pendingRequests[requestKey{queryKey: queryKey, fragmentID: req.fragment.FragmentID}] = req
	})
}

// This method doesn't do removal from the queue.
func (s *Scheduler) cancelRequestAndRemoveFromPending(frontendAddr string, queryID uint64, fragmentID uint64, cancelAll bool) {
	s.pendingRequestsMu.Lock()
	defer s.pendingRequestsMu.Unlock()

	querykey := queryKey{frontendAddr: frontendAddr, queryID: queryID}

	if cancelAll {
		// cancel all requests under the queryID
		for _, fragID := range s.queryFragmentRegistry[querykey] {
			key := requestKey{queryKey: querykey, fragmentID: fragID}
			if req := s.pendingRequests[key]; req != nil {
				req.ctxCancel()
			}
			delete(s.pendingRequests, key)
		}
		delete(s.queryFragmentRegistry, querykey)
	} else {
		// cancel specific fragment of the query by its queryID and fragmentID
		key := requestKey{queryKey: querykey, fragmentID: fragmentID}
		if req := s.pendingRequests[key]; req != nil {
			req.ctxCancel()
		}
		delete(s.pendingRequests, key)
	}
}

// QuerierLoop is started by querier to receive queries from scheduler.
func (s *Scheduler) QuerierLoop(querier schedulerpb.SchedulerForQuerier_QuerierLoopServer) error {
	resp, err := querier.Recv()
	if err != nil {
		return err
	}

	querierID := resp.GetQuerierID()

	s.requestQueue.RegisterQuerierConnection(querierID)
	defer s.requestQueue.UnregisterQuerierConnection(querierID)

	// If the downstream connection to querier is cancelled,
	// we need to ping the condition variable to unblock getNextRequestForQuerier.
	// Ideally we'd have ctx aware condition variables...
	go func() {
		<-querier.Context().Done()
		s.requestQueue.QuerierDisconnecting()
	}()

	lastUserIndex := queue.FirstUser()

	// In stopping state scheduler is not accepting new queries, but still dispatching queries in the queues.
	for s.isRunningOrStopping() {
		req, idx, err := s.requestQueue.GetNextRequestForQuerier(querier.Context(), lastUserIndex, querierID)
		if err != nil {
			return err
		}
		lastUserIndex = idx

		r := req.(*schedulerRequest)

		s.queueDuration.Observe(time.Since(r.enqueueTime).Seconds())
		r.queueSpan.Finish()

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

		if r.ctx.Err() != nil {
			s.cancelRequestAndRemoveFromPending(r.frontendAddress, r.queryID, r.fragment.FragmentID, false)

			lastUserIndex = lastUserIndex.ReuseLastUser()
			continue
		}

		if err := s.forwardRequestToQuerier(querier, r, resp.GetQuerierAddress()); err != nil {
			return err
		}
	}

	return errSchedulerIsNotRunning
}

func (s *Scheduler) NotifyQuerierShutdown(_ context.Context, req *schedulerpb.NotifyQuerierShutdownRequest) (*schedulerpb.NotifyQuerierShutdownResponse, error) {
	level.Info(s.log).Log("msg", "received shutdown notification from querier", "querier", req.GetQuerierID())
	s.requestQueue.NotifyQuerierShutdown(req.GetQuerierID())

	return &schedulerpb.NotifyQuerierShutdownResponse{}, nil
}

func (s *Scheduler) forwardRequestToQuerier(querier schedulerpb.SchedulerForQuerier_QuerierLoopServer, req *schedulerRequest, QuerierAddress string) error {
	// Make sure to cancel request at the end to cleanup resources.
	defer s.cancelRequestAndRemoveFromPending(req.frontendAddress, req.queryID, req.fragment.FragmentID, false)

	// Handle the stream sending & receiving on a goroutine so we can
	// monitoring the contexts in a select and cancel things appropriately.
	errCh := make(chan error, 1)
	go func() {
		childIDtoAddrs := make(map[uint64]string)
		if len(req.fragment.ChildIDs) != 0 {
			for _, childID := range req.fragment.ChildIDs {
				addr, ok := s.fragmentTable.GetAddrByID(req.queryID, childID)
				if !ok {
					errCh <- fmt.Errorf("cannot find child addr for parent fragment %d", req.fragment.FragmentID)
					return
				}
				childIDtoAddrs[childID] = addr
			}
		}

		err := querier.Send(&schedulerpb.SchedulerToQuerier{
			UserID:          req.userID,
			QueryID:         req.queryID,
			FrontendAddress: req.frontendAddress,
			HttpRequest:     req.request,
			StatsEnabled:    req.statsEnabled,
			FragmentID:      req.fragment.FragmentID,
			ChildIDtoAddrs:  childIDtoAddrs,
			IsRoot:          req.fragment.IsRoot,
		})

		if s.distributedExecEnabled {
			s.fragmentTable.AddAddressByID(req.queryID, req.fragment.FragmentID, QuerierAddress)
		}

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

		if err != nil {
			s.forwardErrorToFrontend(req.ctx, req, err)
		}
		return err
	}
}

func (s *Scheduler) forwardErrorToFrontend(ctx context.Context, req *schedulerRequest, requestErr error) {
	opts, err := s.cfg.GRPCClientConfig.DialOption([]grpc.UnaryClientInterceptor{
		otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()),
		middleware.ClientUserHeaderInterceptor},
		nil)
	if err != nil {
		level.Warn(s.log).Log("msg", "failed to create gRPC options for the connection to frontend to report error", "frontend", req.frontendAddress, "err", err, "requestErr", requestErr)
		return
	}

	conn, err := grpc.NewClient(req.frontendAddress, opts...)
	if err != nil {
		level.Warn(s.log).Log("msg", "failed to create gRPC connection to frontend to report error", "frontend", req.frontendAddress, "err", err, "requestErr", requestErr)
		return
	}

	defer func() {
		_ = conn.Close()
	}()

	client := frontendv2pb.NewFrontendForQuerierClient(conn)

	userCtx := user.InjectOrgID(ctx, req.userID)
	_, err = client.QueryResult(userCtx, &frontendv2pb.QueryResultRequest{
		QueryID: req.queryID,
		HttpResponse: &httpgrpc.HTTPResponse{
			Code: http.StatusInternalServerError,
			Body: []byte(requestErr.Error()),
		},
	})

	if err != nil {
		level.Warn(s.log).Log("msg", "failed to forward error to frontend", "frontend", req.frontendAddress, "err", err, "requestErr", requestErr)
		return
	}
}

func (s *Scheduler) isRunningOrStopping() bool {
	st := s.State()
	return st == services.Running || st == services.Stopping
}

func (s *Scheduler) starting(ctx context.Context) error {
	s.subservicesWatcher.WatchManager(s.subservices)

	if err := services.StartManagerAndAwaitHealthy(ctx, s.subservices); err != nil {
		return errors.Wrap(err, "unable to start scheduler subservices")
	}

	return nil
}

func (s *Scheduler) running(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-s.subservicesWatcher.Chan():
			return errors.Wrap(err, "scheduler subservice failed")
		}
	}
}

// Close the Scheduler.
func (s *Scheduler) stopping(_ error) error {
	// This will also stop the requests queue, which stop accepting new requests and errors out any pending requests.
	return services.StopManagerAndAwaitStopped(context.Background(), s.subservices)
}

func (s *Scheduler) cleanupMetricsForInactiveUser(user string) {
	s.queueLength.DeletePartialMatch(prometheus.Labels{
		"user": user,
	})
	s.discardedRequests.DeletePartialMatch(prometheus.Labels{
		"user": user,
	})
	s.requestQueue.CleanupInactiveUserMetrics(user)
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
