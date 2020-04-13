package frontend

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/httpgrpc/server"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/util"
)

var (
	backoffConfig = util.BackoffConfig{
		MinBackoff: 50 * time.Millisecond,
		MaxBackoff: 1 * time.Second,
	}

	errGracefulQuit = errors.New("processor quitting gracefully")
)

type frontendManager struct {
	client FrontendClient
	server *server.Server

	log            log.Logger
	maxSendMsgSize int

	gracefulQuit      []chan struct{}
	serverCtx         context.Context
	cancel            context.CancelFunc
	wg                sync.WaitGroup
	currentProcessors *atomic.Int32
}

func newFrontendManager(serverCtx context.Context, log log.Logger, server *server.Server, client FrontendClient, initialConcurrentRequests int, maxSendMsgSize int) *frontendManager {
	serverCtx, cancel := context.WithCancel(serverCtx)

	f := &frontendManager{
		client:            client,
		log:               log,
		server:            server,
		serverCtx:         serverCtx,
		cancel:            cancel,
		maxSendMsgSize:    maxSendMsgSize,
		currentProcessors: atomic.NewInt32(0),
	}

	f.concurrentRequests(initialConcurrentRequests)

	return f
}

func (f *frontendManager) stop() {
	f.cancel()              // force stop
	f.concurrentRequests(0) // graceful quit
	f.wg.Wait()
}

func (f *frontendManager) concurrentRequests(n int) {
	if n < 0 {
		n = 0
	}

	// adjust clients slice as necessary
	for len(f.gracefulQuit) != n {
		if len(f.gracefulQuit) < n {
			quit := make(chan struct{})
			f.gracefulQuit = append(f.gracefulQuit, quit)

			go f.runOne(quit)

			continue
		}

		if len(f.gracefulQuit) > n {
			// remove from slice and shutdown
			var quit chan struct{}
			quit, f.gracefulQuit = f.gracefulQuit[0], f.gracefulQuit[1:]
			close(quit)
		}
	}
}

// runOne loops, trying to establish a stream to the frontend to begin
// request processing.
//  Ways that this can be cancelled
//   servCtx is cancelled => Cortex is shutting down.
//   c.Recv() errors => transient network issue, client timeout
//   close quit channel => frontendManager is politely asking to shutdown a processor
func (f *frontendManager) runOne(quit <-chan struct{}) {
	f.wg.Add(1)
	defer f.wg.Done()

	f.currentProcessors.Inc()
	defer f.currentProcessors.Dec()

	backoff := util.NewBackoff(f.serverCtx, backoffConfig)
	for backoff.Ongoing() {

		c, err := f.client.Process(f.serverCtx)
		if err != nil {
			level.Error(f.log).Log("msg", "error contacting frontend", "err", err)
			backoff.Wait()
			continue
		}

		if err := f.process(quit, c); err != nil {
			if err == errGracefulQuit {
				level.Debug(f.log).Log("msg", "gracefully shutting down processor")
				return
			}

			level.Error(f.log).Log("msg", "error processing requests", "err", err)
			backoff.Wait()
			continue
		}

		backoff.Reset()
	}
}

// process loops processing requests on an established stream.
func (f *frontendManager) process(quit <-chan struct{}, c Frontend_ProcessClient) error {
	// Build a child context so we can cancel querie when the stream is closed.
	ctx, cancel := context.WithCancel(c.Context())
	defer cancel()

	for {
		select {
		case <-quit:
			return errGracefulQuit
		default:
		}

		request, err := c.Recv()
		if err != nil {
			return err
		}

		// Handle the request on a "background" goroutine, so we go back to
		// blocking on c.Recv().  This allows us to detect the stream closing
		// and cancel the query.  We don't actally handle queries in parallel
		// here, as we're running in lock step with the server - each Recv is
		// paired with a Send.
		go func() {
			response, err := f.server.Handle(ctx, request.HttpRequest)
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
			if len(response.Body) >= f.maxSendMsgSize {
				errMsg := fmt.Sprintf("response larger than the max (%d vs %d)", len(response.Body), f.maxSendMsgSize)
				response = &httpgrpc.HTTPResponse{
					Code: http.StatusRequestEntityTooLarge,
					Body: []byte(errMsg),
				}
				level.Error(f.log).Log("msg", "error processing query", "err", errMsg)
			}

			if err := c.Send(&ProcessResponse{
				HttpResponse: response,
			}); err != nil {
				level.Error(f.log).Log("msg", "error processing requests", "err", err)
			}
		}()
	}
}
