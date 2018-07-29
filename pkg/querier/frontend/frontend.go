package frontend

import (
	"flag"
	"math/rand"
	"net/http"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/httpgrpc/server"
	"github.com/weaveworks/common/user"
)

var (
	errServerClosing  = httpgrpc.Errorf(http.StatusTeapot, "server closing down")
	errTooManyRequest = httpgrpc.Errorf(http.StatusTooManyRequests, "too many outstanding requests")
	errCanceled       = httpgrpc.Errorf(http.StatusInternalServerError, "context cancelled")
)

// Config for a Frontend.
type Config struct {
	MaxOutstandingPerTenant int
	MaxRetries              int
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.MaxOutstandingPerTenant, "querier.max-outstanding-requests-per-tenant", 100, "")
	f.IntVar(&cfg.MaxRetries, "querier.max-retries-per-request", 5, "")
}

// Frontend queues HTTP requests, dispatches them to backends, and handles retries
// for requests which failed.
type Frontend struct {
	cfg Config
	log log.Logger

	mtx    sync.Mutex
	cond   *sync.Cond
	closed bool
	queues map[string]chan *request
}

type request struct {
	request  *httpgrpc.HTTPRequest
	err      chan error
	response chan *httpgrpc.HTTPResponse
}

// New creates a new frontend.
func New(cfg Config, log log.Logger) (*Frontend, error) {
	f := &Frontend{
		cfg:    cfg,
		log:    log,
		queues: map[string]chan *request{},
	}
	f.cond = sync.NewCond(&f.mtx)
	return f, nil
}

// Close stops new requests and errors out any pending requests.
func (f *Frontend) Close() {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	f.closed = true
	f.cond.Broadcast()

	for _, queue := range f.queues {
		close(queue)
		for request := range queue {
			request.err <- errServerClosing
		}
	}
}

// ServeHTTP serves HTTP requests.
func (f *Frontend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := f.serveHTTP(w, r); err != nil {
		server.WriteError(w, err)
	}
}

func (f *Frontend) serveHTTP(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return err
	}

	req, err := server.HTTPRequest(r)
	if err != nil {
		return err
	}

	request := &request{
		request: req,
		// Buffer of 1 to ensure response can be written even if client has gone away.
		err:      make(chan error, 1),
		response: make(chan *httpgrpc.HTTPResponse, 1),
	}

	var lastErr error
	for retries := 0; retries < f.cfg.MaxRetries; retries++ {
		if err := f.queueRequest(userID, request); err != nil {
			return err
		}

		var resp *httpgrpc.HTTPResponse
		select {
		case <-ctx.Done():
			// TODO propagate cancellation.
			//request.Cancel()
			return errCanceled

		case resp = <-request.response:
		case lastErr = <-request.err:
			level.Error(f.log).Log("msg", "error processing request", "try", retries, "err", lastErr)
			resp, _ = httpgrpc.HTTPResponseFromError(lastErr)
		}

		// Only fail is we get a valid HTTP non-500; otherwise retry.
		if resp != nil && resp.Code/100 != 5 {
			server.WriteResponse(w, resp)
			return nil
		}
	}

	return lastErr
}

// Process allows backends to pull requests from the frontend.
func (f *Frontend) Process(server Frontend_ProcessServer) error {
	for {
		request := f.getNextRequest()
		if request == nil {
			// Occurs when server is shutting down.
			return nil
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
	f.mtx.Lock()
	defer f.mtx.Unlock()

	if f.closed {
		return errServerClosing
	}

	queue, ok := f.queues[userID]
	if !ok {
		queue = make(chan *request, f.cfg.MaxOutstandingPerTenant)
		f.queues[userID] = queue
	}

	select {
	case queue <- req:
		f.cond.Signal()
		return nil
	default:
		return errTooManyRequest
	}
}

// getQueue picks a random queue and takes the next request off of it, so we
// faily process users queries.  Will block if there are no requests.
func (f *Frontend) getNextRequest() *request {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	for len(f.queues) == 0 && !f.closed {
		f.cond.Wait()
	}

	if f.closed {
		return nil
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
		return request
	}

	panic("should never happen")
}
