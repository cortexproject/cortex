package frontend2

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/cortexproject/cortex/pkg/frontend/v2/frontendv2pb"
	"github.com/cortexproject/cortex/pkg/ring/client"
	"github.com/cortexproject/cortex/pkg/scheduler/schedulerpb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/grpcclient"
	"github.com/cortexproject/cortex/pkg/util/grpcutil"
	cortex_middleware "github.com/cortexproject/cortex/pkg/util/middleware"
	"github.com/cortexproject/cortex/pkg/util/services"
)

// Handler for HTTP requests wrapped in protobuf messages.
type RequestHandler interface {
	Handle(context.Context, *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, error)
}

type QuerierWorkersConfig struct {
	SchedulerAddress string        `yaml:"scheduler_address"`
	DNSLookupPeriod  time.Duration `yaml:"scheduler_dns_lookup_period"`

	// Following settings are not exposed via YAML or CLI Flags, but instead copied from "v1" worker config.
	GRPCClientConfig      grpcclient.ConfigWithTLS `yaml:"-"` // In v1 this is called "frontend client", here we use it for scheduler.
	MatchMaxConcurrency   bool                     `yaml:"-"`
	MaxConcurrentRequests int                      `yaml:"-"` // Must be same as passed to PromQL Engine.
	Parallelism           int                      `yaml:"-"`
	QuerierID             string                   `yaml:"-"` // ID to pass to scheduler when connecting.
}

func (cfg *QuerierWorkersConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.SchedulerAddress, "querier.scheduler-address", "", "Hostname (and port) of scheduler that querier will periodically resolve, connect to and receive queries from. If set, takes precedence over -querier.frontend-address.")
	f.DurationVar(&cfg.DNSLookupPeriod, "querier.scheduler-dns-lookup-period", 10*time.Second, "How often to resolve the scheduler-address, in order to look for new query-scheduler instances.")
}

type querierSchedulerWorkers struct {
	*services.BasicService

	cfg            QuerierWorkersConfig
	requestHandler RequestHandler

	log log.Logger

	subservices *services.Manager

	frontendPool                  *client.Pool
	frontendClientRequestDuration *prometheus.HistogramVec

	mu sync.Mutex
	// Set to nil when stop is called... no more workers are created afterwards.
	workers map[string]*querierSchedulerWorker
}

func NewQuerierSchedulerWorkers(cfg QuerierWorkersConfig, handler RequestHandler, reg prometheus.Registerer, log log.Logger) (services.Service, error) {
	if cfg.SchedulerAddress == "" {
		return nil, errors.New("no scheduler address")
	}

	if cfg.QuerierID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, errors.Wrap(err, "failed to get hostname for configuring querier ID")
		}
		cfg.QuerierID = hostname
	}

	frontendClientsGauge := promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "cortex_querier_query_frontend_clients",
		Help: "The current number of clients connected to query-frontend.",
	})

	f := &querierSchedulerWorkers{
		cfg:            cfg,
		log:            log,
		requestHandler: handler,
		workers:        map[string]*querierSchedulerWorker{},

		frontendClientRequestDuration: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cortex_querier_query_frontend_request_duration_seconds",
			Help:    "Time spend doing requests to frontend.",
			Buckets: prometheus.ExponentialBuckets(0.001, 4, 6),
		}, []string{"operation", "status_code"}),
	}

	poolConfig := client.PoolConfig{
		CheckInterval:      5 * time.Second,
		HealthCheckEnabled: true,
		HealthCheckTimeout: 1 * time.Second,
	}

	p := client.NewPool("frontend", poolConfig, nil, f.createFrontendClient, frontendClientsGauge, log)
	f.frontendPool = p

	w, err := util.NewDNSWatcher(cfg.SchedulerAddress, cfg.DNSLookupPeriod, f)
	if err != nil {
		return nil, err
	}

	f.subservices, err = services.NewManager(w, p)
	if err != nil {
		return nil, errors.Wrap(err, "querier scheduler worker subservices")
	}
	f.BasicService = services.NewIdleService(f.starting, f.stopping)
	return f, nil
}

func (f *querierSchedulerWorkers) starting(ctx context.Context) error {
	return services.StartManagerAndAwaitHealthy(ctx, f.subservices)
}

func (f *querierSchedulerWorkers) stopping(_ error) error {
	return services.StopManagerAndAwaitStopped(context.Background(), f.subservices)
}

func (f *querierSchedulerWorkers) AddressAdded(address string) {
	ctx := f.ServiceContext()
	if ctx == nil || ctx.Err() != nil {
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// We already have worker for this scheduler.
	if w := f.workers[address]; w != nil {
		return
	}

	level.Debug(f.log).Log("msg", "adding connection to scheduler", "addr", address)
	conn, err := f.connectToScheduler(context.Background(), address)
	if err != nil {
		level.Error(f.log).Log("msg", "error connecting to scheduler", "addr", address, "err", err)
		return
	}

	// If not, start a new one.
	f.workers[address] = newQuerierSchedulerWorker(ctx, conn, address, f.requestHandler, f.cfg.GRPCClientConfig.GRPC.MaxSendMsgSize, f.cfg.QuerierID, f.frontendPool, f.log)
	f.resetConcurrency() // Called with lock.
}

// Requires lock.
func (f *querierSchedulerWorkers) resetConcurrency() {
	totalConcurrency := 0
	index := 0
	for _, w := range f.workers {
		concurrency := 0

		if f.cfg.MatchMaxConcurrency {
			concurrency = f.cfg.MaxConcurrentRequests / len(f.workers)

			// If max concurrency does not evenly divide into our frontends a subset will be chosen
			// to receive an extra connection.  Frontend addresses were shuffled above so this will be a
			// random selection of frontends.
			if index < f.cfg.MaxConcurrentRequests%len(f.workers) {
				concurrency++
			}

			// If concurrentRequests is 0 then MaxConcurrentRequests is less than the total number of
			// schedulers. In order to prevent accidentally starving a scheduler we are just going to
			// always connect once to every scheduler.  This is dangerous b/c we may start exceeding PromQL
			// max concurrency.
			if concurrency == 0 {
				concurrency = 1
			}
		} else {
			concurrency = f.cfg.Parallelism
		}

		totalConcurrency += concurrency
		w.concurrency(concurrency)
		index++
	}

	if totalConcurrency > f.cfg.MaxConcurrentRequests {
		level.Warn(f.log).Log("msg", "total worker concurrency is greater than promql max concurrency. queries may be queued in the querier which reduces QOS")
	}
}

func (f *querierSchedulerWorkers) AddressRemoved(address string) {
	level.Debug(f.log).Log("msg", "removing connection to scheduler", "addr", address)

	f.mu.Lock()
	w := f.workers[address]
	delete(f.workers, address)
	f.mu.Unlock()

	if w != nil {
		w.stop()
	}
}

func (f *querierSchedulerWorkers) createFrontendClient(addr string) (client.PoolClient, error) {
	opts, err := f.cfg.GRPCClientConfig.DialOption([]grpc.UnaryClientInterceptor{
		otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()),
		middleware.ClientUserHeaderInterceptor,
		cortex_middleware.PrometheusGRPCUnaryInstrumentation(f.frontendClientRequestDuration),
	}, nil)

	if err != nil {
		return nil, err
	}

	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}

	return &frontendClient{
		FrontendForQuerierClient: frontendv2pb.NewFrontendForQuerierClient(conn),
		HealthClient:             grpc_health_v1.NewHealthClient(conn),
		conn:                     conn,
	}, nil
}

func (f *querierSchedulerWorkers) connectToScheduler(ctx context.Context, address string) (*grpc.ClientConn, error) {
	// Because we only use single long-running method, it doesn't make sense to inject user ID, send over tracing or add metrics.
	opts, err := f.cfg.GRPCClientConfig.DialOption(nil, nil)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.DialContext(ctx, address, opts...)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// Worker manages connection to single scheduler, and runs multiple goroutines for handling PromQL requests.
type querierSchedulerWorker struct {
	log log.Logger

	schedulerAddress string
	handler          RequestHandler
	maxMessageSize   int
	querierID        string

	// Main context to control all goroutines.
	ctx context.Context
	wg  sync.WaitGroup

	conn         *grpc.ClientConn
	frontendPool *client.Pool

	// Cancel functions for individual goroutines.
	cancelsMu sync.Mutex
	cancels   []context.CancelFunc
}

func newQuerierSchedulerWorker(ctx context.Context, conn *grpc.ClientConn, schedulerAddr string, requestHandler RequestHandler, maxMessageSize int, querierID string, frontendPool *client.Pool, log log.Logger) *querierSchedulerWorker {
	w := &querierSchedulerWorker{
		schedulerAddress: schedulerAddr,
		conn:             conn,
		handler:          requestHandler,
		maxMessageSize:   maxMessageSize,
		querierID:        querierID,

		ctx:          ctx,
		log:          log,
		frontendPool: frontendPool,
	}
	return w
}

func (w *querierSchedulerWorker) stop() {
	w.concurrency(0)

	// And wait until they finish.
	w.wg.Wait()
}

func (w *querierSchedulerWorker) concurrency(n int) {
	w.cancelsMu.Lock()
	defer w.cancelsMu.Unlock()

	if n < 0 {
		n = 0
	}

	for len(w.cancels) < n {
		ctx, cancel := context.WithCancel(w.ctx)
		w.cancels = append(w.cancels, cancel)

		w.wg.Add(1)
		go w.runOne(ctx)
	}

	for len(w.cancels) > n {
		w.cancels[0]()
		w.cancels = w.cancels[1:]
	}
}

// runOne loops, trying to establish a stream to the frontend to begin
// request processing.
func (w *querierSchedulerWorker) runOne(ctx context.Context) {
	defer w.wg.Done()

	schedulerClient := schedulerpb.NewSchedulerForQuerierClient(w.conn)

	backoffConfig := util.BackoffConfig{
		MinBackoff: 50 * time.Millisecond,
		MaxBackoff: 1 * time.Second,
	}

	backoff := util.NewBackoff(ctx, backoffConfig)
	for backoff.Ongoing() {
		c, err := schedulerClient.QuerierLoop(ctx)
		if err == nil {
			err = c.Send(&schedulerpb.QuerierToScheduler{QuerierID: w.querierID})
		}

		if err != nil {
			level.Error(w.log).Log("msg", "error contacting scheduler", "err", err, "addr", w.schedulerAddress)
			backoff.Wait()
			continue
		}

		if err := w.querierLoop(c); err != nil {
			level.Error(w.log).Log("msg", "error processing requests from scheduler", "err", err, "addr", w.schedulerAddress)
			backoff.Wait()
			continue
		}

		backoff.Reset()
	}
}

// process loops processing requests on an established stream.
func (w *querierSchedulerWorker) querierLoop(c schedulerpb.SchedulerForQuerier_QuerierLoopClient) error {
	// Build a child context so we can cancel a query when the stream is closed.
	ctx, cancel := context.WithCancel(c.Context())
	defer cancel()

	for {
		request, err := c.Recv()
		if err != nil {
			return err
		}

		// Handle the request on a "background" goroutine, so we go back to
		// blocking on c.Recv().  This allows us to detect the stream closing
		// and cancel the query.  We don't actually handle queries in parallel
		// here, as we're running in lock step with the server - each Recv is
		// paired with a Send.
		go func() {
			// We need to inject user into context for sending response back.
			ctx := user.InjectOrgID(ctx, request.UserID)

			tracer := opentracing.GlobalTracer()
			// Ignore errors here. If we cannot get parent span, we just don't create new one.
			parentSpanContext, _ := grpcutil.GetParentSpanForRequest(tracer, request.HttpRequest)
			if parentSpanContext != nil {
				queueSpan, spanCtx := opentracing.StartSpanFromContextWithTracer(ctx, tracer, "querier_worker_runRequest", opentracing.ChildOf(parentSpanContext))
				defer queueSpan.Finish()

				ctx = spanCtx
			}
			logger := util.WithContext(ctx, w.log)

			w.runRequest(ctx, logger, request.QueryID, request.FrontendAddress, request.HttpRequest)

			// Report back to scheduler that processing of the query has finished.
			if err := c.Send(&schedulerpb.QuerierToScheduler{}); err != nil {
				level.Error(logger).Log("msg", "error notifying scheduler about finished query", "err", err, "addr", w.schedulerAddress)
			}
		}()
	}
}

func (w *querierSchedulerWorker) runRequest(ctx context.Context, logger log.Logger, queryID uint64, frontendAddress string, request *httpgrpc.HTTPRequest) {
	response, err := w.handler.Handle(ctx, request)
	if err != nil {
		var ok bool
		response, ok = httpgrpc.HTTPResponseFromError(err)
		if !ok {
			response = &httpgrpc.HTTPResponse{
				Code: http.StatusInternalServerError,
				Body: []byte(err.Error()),
			}
		}
	}

	// Ensure responses that are too big are not retried.
	if len(response.Body) >= w.maxMessageSize {
		level.Error(logger).Log("msg", "response larger than max message size", "size", len(response.Body), "maxMessageSize", w.maxMessageSize)

		errMsg := fmt.Sprintf("response larger than the max message size (%d vs %d)", len(response.Body), w.maxMessageSize)
		response = &httpgrpc.HTTPResponse{
			Code: http.StatusRequestEntityTooLarge,
			Body: []byte(errMsg),
		}
	}

	c, err := w.frontendPool.GetClientFor(frontendAddress)
	if err == nil {
		// Response is empty and uninteresting.
		_, err = c.(frontendv2pb.FrontendForQuerierClient).QueryResult(ctx, &frontendv2pb.QueryResultRequest{
			QueryID:      queryID,
			HttpResponse: response,
		})
	}
	if err != nil {
		level.Error(logger).Log("msg", "error notifying frontend about finished query", "err", err, "frontend", frontendAddress)
	}
}

type frontendClient struct {
	frontendv2pb.FrontendForQuerierClient
	grpc_health_v1.HealthClient
	conn *grpc.ClientConn
}

func (fc *frontendClient) Close() error {
	return fc.conn.Close()
}
