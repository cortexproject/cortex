package frontend

import (
	"context"
	"flag"
	"net/http"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/mwitkow/go-grpc-middleware"
	"github.com/opentracing/opentracing-go"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/httpgrpc/server"
	"github.com/weaveworks/common/middleware"
	"google.golang.org/grpc"

	"github.com/weaveworks/cortex/pkg/util"
)

var (
	backoffConfig = util.BackoffConfig{
		MinBackoff: 50 * time.Millisecond,
		MaxBackoff: 1 * time.Second,
	}
)

// WorkerConfig is config for a worker.
type WorkerConfig struct {
	Address     string
	Parallelism int
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *WorkerConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Address, "querier.frontend-address", "", "")
	f.IntVar(&cfg.Parallelism, "querier.worker-parallelism", 1, "")
}

// Worker is the counter-part to the frontend, actually processing requests.
type Worker struct {
	cfg    WorkerConfig
	log    log.Logger
	server *server.Server

	client FrontendClient
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewWorker creates a new Worker.
func NewWorker(cfg WorkerConfig, server *server.Server, log log.Logger) (*Worker, error) {
	client, err := connect(cfg.Address)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	worker := &Worker{
		cfg:    cfg,
		log:    log,
		server: server,

		client: client,
		ctx:    ctx,
		cancel: cancel,
	}
	worker.wg.Add(cfg.Parallelism)
	for i := 0; i < cfg.Parallelism; i++ {
		go worker.run()
	}
	return worker, nil
}

// Stop the worker.
func (w *Worker) Stop() {
	w.cancel()
	w.wg.Wait()
}

// Run infinitely loops, trying to establish a connection to the frontend to
// begin request processing.
func (w *Worker) run() {
	defer w.wg.Done()

	backoff := util.NewBackoff(w.ctx, backoffConfig)
	for backoff.Ongoing() {
		c, err := w.client.Process(w.ctx)
		if err != nil {
			level.Error(w.log).Log("msg", "error contacting frontend", "err", err)
			backoff.Wait()
			continue
		}

		if err := w.loop(w.ctx, c); err != nil {
			level.Error(w.log).Log("msg", "error processing requests", "err", err)
			backoff.Wait()
			continue
		}

		backoff.Reset()
	}
}

func connect(address string) (FrontendClient, error) {
	conn, err := grpc.Dial(
		address,
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
			otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()),
			middleware.ClientUserHeaderInterceptor,
		)),
	)
	if err != nil {
		return nil, err
	}
	return NewFrontendClient(conn), nil
}

func (w *Worker) loop(ctx context.Context, c Frontend_ProcessClient) error {
	for {
		request, err := c.Recv()
		if err != nil {
			return err
		}

		response, err := w.server.Handle(ctx, request.HttpRequest)
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

		if err := c.Send(&ProcessResponse{
			HttpResponse: response,
		}); err != nil {
			return err
		}
	}
}
