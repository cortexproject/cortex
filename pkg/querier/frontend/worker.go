package frontend

import (
	"context"
	"flag"
	"net/http"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/mwitkow/go-grpc-middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/naming"

	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/httpgrpc/server"
	"github.com/weaveworks/common/middleware"
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
	Address           string
	Parallelism       int
	DNSLookupDuration time.Duration
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *WorkerConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Address, "querier.frontend-address", "", "Address of query frontend service.")
	f.IntVar(&cfg.Parallelism, "querier.worker-parallelism", 10, "Number of simultaneous queries to process.")
	f.DurationVar(&cfg.DNSLookupDuration, "querier.dns-lookup-period", 10*time.Second, "How often to query DNS.")
}

// Worker is the counter-part to the frontend, actually processing requests.
type Worker interface {
	Stop()
}

type worker struct {
	cfg    WorkerConfig
	log    log.Logger
	server *server.Server

	ctx     context.Context
	cancel  context.CancelFunc
	watcher naming.Watcher
	wg      sync.WaitGroup
}

type noopWorker struct {
}

func (noopWorker) Stop() {}

// NewWorker creates a new Worker.
func NewWorker(cfg WorkerConfig, server *server.Server, log log.Logger) (Worker, error) {
	if cfg.Address == "" {
		level.Info(log).Log("msg", "no address specified, not starting worker")
		return noopWorker{}, nil
	}

	resolver, err := naming.NewDNSResolverWithFreq(cfg.DNSLookupDuration)
	if err != nil {
		return nil, err
	}

	watcher, err := resolver.Resolve(cfg.Address)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	w := &worker{
		cfg:    cfg,
		log:    log,
		server: server,

		ctx:     ctx,
		cancel:  cancel,
		watcher: watcher,
	}
	w.wg.Add(1)
	go w.watchDNSLoop()
	return w, nil
}

// Stop the worker.
func (w *worker) Stop() {
	w.watcher.Close()
	w.cancel()
	w.wg.Wait()
}

// watchDNSLoop watches for changes in DNS and starts or stops workers.
func (w *worker) watchDNSLoop() {
	defer w.wg.Done()

	cancels := map[string]context.CancelFunc{}
	defer func() {
		for _, cancel := range cancels {
			cancel()
		}
	}()

	for {
		updates, err := w.watcher.Next()
		if err != nil {
			level.Error(w.log).Log("msg", "error from DNS watcher", "err", err)
			return
		}

		for _, update := range updates {
			switch update.Op {
			case naming.Add:
				level.Debug(w.log).Log("msg", "adding connection", "addr", update.Addr)
				ctx, cancel := context.WithCancel(w.ctx)
				cancels[update.Addr] = cancel
				w.runMany(ctx, update.Addr)

			case naming.Delete:
				level.Debug(w.log).Log("msg", "removing connection", "addr", update.Addr)
				if cancel, ok := cancels[update.Addr]; ok {
					cancel()
				}

			default:
				panic("unknown op")
			}
		}
	}
}

// runMany starts N runOne loops for a given address.
func (w *worker) runMany(ctx context.Context, address string) {
	client, err := connect(address)
	if err != nil {
		level.Error(w.log).Log("msg", "error connecting", "addr", address, "err", err)
		return
	}

	w.wg.Add(w.cfg.Parallelism)
	for i := 0; i < w.cfg.Parallelism; i++ {
		go w.runOne(ctx, client)
	}
}

// runOne loops, trying to establish a stream to the frontend to begin
// request processing.
func (w *worker) runOne(ctx context.Context, client FrontendClient) {
	defer w.wg.Done()

	backoff := util.NewBackoff(ctx, backoffConfig)
	for backoff.Ongoing() {
		c, err := client.Process(ctx)
		if err != nil {
			level.Error(w.log).Log("msg", "error contacting frontend", "err", err)
			backoff.Wait()
			continue
		}

		if err := w.process(ctx, c); err != nil {
			level.Error(w.log).Log("msg", "error processing requests", "err", err)
			backoff.Wait()
			continue
		}

		backoff.Reset()
	}
}

// process loops processing requests on an established stream.
func (w *worker) process(ctx context.Context, c Frontend_ProcessClient) error {
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

func connect(address string) (FrontendClient, error) {
	conn, err := grpc.Dial(
		address,
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
			middleware.ClientUserHeaderInterceptor,
		)),
	)
	if err != nil {
		return nil, err
	}
	return NewFrontendClient(conn), nil
}
