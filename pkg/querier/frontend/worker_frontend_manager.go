package frontend

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/util"
)

type upstream interface {
	Handle(context.Context, *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, error)
}

type frontendManager struct {
	client       FrontendClient
	gracefulQuit []chan struct{}

	server         upstream
	log            log.Logger
	ctx            context.Context
	maxSendMsgSize int

	wg  sync.WaitGroup
	mtx sync.Mutex
}

func NewFrontendManager(ctx context.Context, log log.Logger, server upstream, client FrontendClient, initialConcurrentRequests int, maxSendMsgSize int) *frontendManager {
	f := &frontendManager{
		client:         client,
		ctx:            ctx,
		log:            log,
		server:         server,
		maxSendMsgSize: maxSendMsgSize,
	}

	f.concurrentRequests(initialConcurrentRequests)

	return f
}

func (f *frontendManager) stop() {
	f.concurrentRequests(0)
	f.wg.Wait()
}

func (f *frontendManager) concurrentRequests(n int) error {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	// adjust clients slice as necessary
	for len(f.gracefulQuit) != n {
		if len(f.gracefulQuit) < n {
			quit := make(chan struct{})
			f.gracefulQuit = append(f.gracefulQuit, quit)

			f.runOne(quit)

			continue
		}

		if len(f.gracefulQuit) > n {
			// remove from slice and shutdown
			var quit chan struct{}
			quit, f.gracefulQuit = f.gracefulQuit[0], f.gracefulQuit[1:]
			close(quit)
		}
	}

	return nil
}

// jpe
// is f.wg.Add(1) safe?
// pass graceful quit

// runOne loops, trying to establish a stream to the frontend to begin
// request processing.
func (f *frontendManager) runOne(quit <-chan struct{}) {
	f.wg.Add(1)
	defer f.wg.Done()

	backoff := util.NewBackoff(f.ctx, backoffConfig)
	for backoff.Ongoing() {

		// break context chain here
		c, err := f.client.Process(f.ctx)
		if err != nil {
			level.Error(f.log).Log("msg", "error contacting frontend", "err", err)
			backoff.Wait()
			continue
		}

		if err := f.process(quit, c); err != nil {
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
			return nil // jpe: won't really work with runOne
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
