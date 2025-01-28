package worker

import (
	"context"
	"fmt"
	"net/http"
	"net/textproto"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/frontend/v1/frontendv1pb"
	querier_stats "github.com/cortexproject/cortex/pkg/querier/stats"
	"github.com/cortexproject/cortex/pkg/util/backoff"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
)

var (
	processorBackoffConfig = backoff.Config{
		MinBackoff: 50 * time.Millisecond,
		MaxBackoff: 1 * time.Second,
	}
)

func newFrontendProcessor(cfg Config, handler RequestHandler, log log.Logger) processor {
	return &frontendProcessor{
		log:            log,
		handler:        handler,
		maxMessageSize: cfg.GRPCClientConfig.MaxSendMsgSize,
		querierID:      cfg.QuerierID,
		targetHeaders:  cfg.TargetHeaders,
	}
}

// Handles incoming queries from frontend.
type frontendProcessor struct {
	handler        RequestHandler
	maxMessageSize int
	querierID      string

	log log.Logger

	targetHeaders []string
}

// notifyShutdown implements processor.
func (fp *frontendProcessor) notifyShutdown(ctx context.Context, conn *grpc.ClientConn, address string) {
	client := frontendv1pb.NewFrontendClient(conn)

	req := &frontendv1pb.NotifyClientShutdownRequest{ClientID: fp.querierID}
	if _, err := client.NotifyClientShutdown(ctx, req); err != nil {
		// Since we're shutting down there's nothing we can do except logging it.
		level.Warn(fp.log).Log("msg", "failed to notify querier shutdown to query-frontend", "address", address, "err", err)
	}
}

// runOne loops, trying to establish a stream to the frontend to begin request processing.
func (fp *frontendProcessor) processQueriesOnSingleStream(ctx context.Context, conn *grpc.ClientConn, address string) {
	client := frontendv1pb.NewFrontendClient(conn)

	backoff := backoff.New(ctx, processorBackoffConfig)
	for backoff.Ongoing() {
		c, err := client.Process(ctx)
		if err != nil {
			level.Error(fp.log).Log("msg", "error contacting frontend", "address", address, "err", err)
			backoff.Wait()
			continue
		}

		if err := fp.process(c); err != nil {
			level.Error(fp.log).Log("msg", "error processing requests", "address", address, "err", err)
			backoff.Wait()
			continue
		}

		backoff.Reset()
	}
}

// process loops processing requests on an established stream.
func (fp *frontendProcessor) process(c frontendv1pb.Frontend_ProcessClient) error {
	// Build a child context so we can cancel a query when the stream is closed.
	ctx, cancel := context.WithCancel(c.Context())
	defer cancel()

	for {
		request, err := c.Recv()
		if err != nil {
			return err
		}

		switch request.Type {
		case frontendv1pb.HTTP_REQUEST:
			// Handle the request on a "background" goroutine, so we go back to
			// blocking on c.Recv().  This allows us to detect the stream closing
			// and cancel the query.  We don't actually handle queries in parallel
			// here, as we're running in lock step with the server - each Recv is
			// paired with a Send.
			go fp.runRequest(ctx, request.HttpRequest, request.StatsEnabled, func(response *httpgrpc.HTTPResponse, stats *querier_stats.QueryStats) error {
				return c.Send(&frontendv1pb.ClientToFrontend{
					HttpResponse: response,
					Stats:        stats,
				})
			})

		case frontendv1pb.GET_ID:
			err := c.Send(&frontendv1pb.ClientToFrontend{ClientID: fp.querierID})
			if err != nil {
				return err
			}

		default:
			return fmt.Errorf("unknown request type: %v", request.Type)
		}
	}
}

func (fp *frontendProcessor) runRequest(ctx context.Context, request *httpgrpc.HTTPRequest, statsEnabled bool, sendHTTPResponse func(response *httpgrpc.HTTPResponse, stats *querier_stats.QueryStats) error) {
	var stats *querier_stats.QueryStats
	if statsEnabled {
		stats, ctx = querier_stats.ContextWithEmptyStats(ctx)
	}

	headers := make(map[string]string, 0)
	for _, h := range request.Headers {
		headers[h.Key] = h.Values[0]
	}
	headerMap := make(map[string]string, 0)
	// Remove non-existent header.
	for _, header := range fp.targetHeaders {
		if v, ok := headers[textproto.CanonicalMIMEHeaderKey(header)]; ok {
			headerMap[header] = v
		}
	}
	orgID, ok := headers[textproto.CanonicalMIMEHeaderKey(user.OrgIDHeaderName)]
	if ok {
		ctx = user.InjectOrgID(ctx, orgID)
	}
	ctx = util_log.ContextWithHeaderMap(ctx, headerMap)
	logger := util_log.WithContext(ctx, fp.log)
	if statsEnabled {
		level.Info(logger).Log("msg", "started running request")
	}

	response, err := fp.handler.Handle(ctx, request)
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
	if statsEnabled {
		level.Info(logger).Log("msg", "finished request", "status_code", response.Code, "response_size", len(response.GetBody()))
	}

	// Ensure responses that are too big are not retried.
	if len(response.Body) >= fp.maxMessageSize {
		errMsg := fmt.Sprintf("response larger than the max (%d vs %d)", len(response.Body), fp.maxMessageSize)
		response = &httpgrpc.HTTPResponse{
			Code: http.StatusRequestEntityTooLarge,
			Body: []byte(errMsg),
		}
		level.Error(fp.log).Log("msg", "error processing query", "err", errMsg)
	}

	if err := sendHTTPResponse(response, stats); err != nil {
		level.Error(fp.log).Log("msg", "error processing requests", "err", err)
	}
}
