package frontend2

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/opentracing/opentracing-go"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-client-go/config"
	"github.com/weaveworks/common/httpgrpc"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
)

const testMaxOutstandingPerTenant = 5

func setupScheduler(t *testing.T) (*Scheduler, SchedulerForFrontendClient, SchedulerForQuerierClient) {
	s, err := NewScheduler(SchedulerConfig{MaxOutstandingPerTenant: testMaxOutstandingPerTenant}, &limits{queriers: 2}, log.NewNopLogger(), nil)
	require.NoError(t, err)

	server := grpc.NewServer()
	RegisterSchedulerForFrontendServer(server, s)
	RegisterSchedulerForQuerierServer(server, s)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), s))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), s)
	})

	l, err := net.Listen("tcp", "")
	require.NoError(t, err)

	go func() {
		_ = server.Serve(l)
	}()

	t.Cleanup(func() {
		_ = l.Close()
	})

	c, err := grpc.Dial(l.Addr().String(), grpc.WithInsecure())
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = c.Close()
	})

	return s, NewSchedulerForFrontendClient(c), NewSchedulerForQuerierClient(c)
}

func TestSchedulerBasicEnqueue(t *testing.T) {
	scheduler, frontendClient, querierClient := setupScheduler(t)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
	frontendToScheduler(t, frontendLoop, &FrontendToScheduler{
		Type:        ENQUEUE,
		QueryID:     1,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
	})

	{
		querierLoop, err := querierClient.QuerierLoop(context.Background())
		require.NoError(t, err)
		require.NoError(t, querierLoop.Send(&QuerierToScheduler{QuerierID: "querier-1"}))

		msg2, err := querierLoop.Recv()
		require.NoError(t, err)
		require.Equal(t, uint64(1), msg2.QueryID)
		require.Equal(t, "frontend-12345", msg2.FrontendAddress)
		require.Equal(t, "GET", msg2.HttpRequest.Method)
		require.Equal(t, "/hello", msg2.HttpRequest.Url)
		require.NoError(t, querierLoop.Send(&QuerierToScheduler{}))
	}

	verifyNoPendingRequestsLeft(t, scheduler)
}

func TestSchedulerEnqueueWithCancel(t *testing.T) {
	scheduler, frontendClient, querierClient := setupScheduler(t)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
	frontendToScheduler(t, frontendLoop, &FrontendToScheduler{
		Type:        ENQUEUE,
		QueryID:     1,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
	})

	frontendToScheduler(t, frontendLoop, &FrontendToScheduler{
		Type:    CANCEL,
		QueryID: 1,
	})

	querierLoop := initQuerierLoop(t, querierClient, "querier-1")

	verifyQuerierDoesntReceiveRequest(t, querierLoop, 500*time.Millisecond)
	verifyNoPendingRequestsLeft(t, scheduler)
}

func initQuerierLoop(t *testing.T, querierClient SchedulerForQuerierClient, querier string) SchedulerForQuerier_QuerierLoopClient {
	querierLoop, err := querierClient.QuerierLoop(context.Background())
	require.NoError(t, err)
	require.NoError(t, querierLoop.Send(&QuerierToScheduler{QuerierID: querier}))

	return querierLoop
}

func TestSchedulerEnqueueByMultipleFrontendsWithCancel(t *testing.T) {
	scheduler, frontendClient, querierClient := setupScheduler(t)

	frontendLoop1 := initFrontendLoop(t, frontendClient, "frontend-1")
	frontendLoop2 := initFrontendLoop(t, frontendClient, "frontend-2")

	frontendToScheduler(t, frontendLoop1, &FrontendToScheduler{
		Type:        ENQUEUE,
		QueryID:     1,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello1"},
	})

	frontendToScheduler(t, frontendLoop2, &FrontendToScheduler{
		Type:        ENQUEUE,
		QueryID:     1,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello2"},
	})

	// Cancel first query by first frontend.
	frontendToScheduler(t, frontendLoop1, &FrontendToScheduler{
		Type:    CANCEL,
		QueryID: 1,
	})

	querierLoop := initQuerierLoop(t, querierClient, "querier-1")

	// Let's verify that we can receive query 1 from frontend-2.
	msg, err := querierLoop.Recv()
	require.NoError(t, err)
	require.Equal(t, uint64(1), msg.QueryID)
	require.Equal(t, "frontend-2", msg.FrontendAddress)
	// Must notify scheduler back about finished processing, or it will not send more requests (nor remove "current" request from pending ones).
	require.NoError(t, querierLoop.Send(&QuerierToScheduler{}))

	// But nothing else.
	verifyQuerierDoesntReceiveRequest(t, querierLoop, 500*time.Millisecond)
	verifyNoPendingRequestsLeft(t, scheduler)
}

func TestSchedulerEnqueueWithFrontendDisconnect(t *testing.T) {
	scheduler, frontendClient, querierClient := setupScheduler(t)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
	frontendToScheduler(t, frontendLoop, &FrontendToScheduler{
		Type:        ENQUEUE,
		QueryID:     1,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
	})

	// Wait until the frontend has connected to the scheduler.
	test.Poll(t, time.Second, float64(1), func() interface{} {
		return promtest.ToFloat64(scheduler.connectedFrontendClients)
	})

	// Disconnect frontend.
	require.NoError(t, frontendLoop.CloseSend())

	// Wait until the frontend has disconnected.
	test.Poll(t, time.Second, float64(0), func() interface{} {
		return promtest.ToFloat64(scheduler.connectedFrontendClients)
	})

	querierLoop := initQuerierLoop(t, querierClient, "querier-1")

	verifyQuerierDoesntReceiveRequest(t, querierLoop, 500*time.Millisecond)
	verifyNoPendingRequestsLeft(t, scheduler)
}

func TestCancelRequestInProgress(t *testing.T) {
	scheduler, frontendClient, querierClient := setupScheduler(t)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
	frontendToScheduler(t, frontendLoop, &FrontendToScheduler{
		Type:        ENQUEUE,
		QueryID:     1,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
	})

	querierLoop, err := querierClient.QuerierLoop(context.Background())
	require.NoError(t, err)
	require.NoError(t, querierLoop.Send(&QuerierToScheduler{QuerierID: "querier-1"}))

	_, err = querierLoop.Recv()
	require.NoError(t, err)

	// At this point, scheduler assumes that querier is processing the request (until it receives empty QuerierToScheduler message back).
	// Simulate frontend disconnect.
	require.NoError(t, frontendLoop.CloseSend())

	// Add a little sleep to make sure that scheduler notices frontend disconnect.
	time.Sleep(500 * time.Millisecond)

	// Report back end of request processing. This should return error, since the QuerierLoop call has finished on scheduler.
	// Note: testing on querierLoop.Context() cancellation didn't work :(
	err = querierLoop.Send(&QuerierToScheduler{})
	require.Error(t, err)

	verifyNoPendingRequestsLeft(t, scheduler)
}

func TestTracingContext(t *testing.T) {
	scheduler, frontendClient, _ := setupScheduler(t)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")

	closer, err := config.Configuration{}.InitGlobalTracer("test")
	require.NoError(t, err)
	defer closer.Close()

	req := &FrontendToScheduler{
		Type:            ENQUEUE,
		QueryID:         1,
		UserID:          "test",
		HttpRequest:     &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
		FrontendAddress: "frontend-12345",
	}

	sp, _ := opentracing.StartSpanFromContext(context.Background(), "client")
	opentracing.GlobalTracer().Inject(sp.Context(), opentracing.HTTPHeaders, (*httpgrpcHeadersCarrier)(req.HttpRequest))

	frontendToScheduler(t, frontendLoop, req)

	scheduler.mtx.Lock()
	defer scheduler.mtx.Unlock()
	require.Equal(t, 1, len(scheduler.pendingRequests))

	for _, r := range scheduler.pendingRequests {
		require.NotNil(t, r.parentSpanContext)
	}
}

func TestSchedulerShutdown_FrontendLoop(t *testing.T) {
	scheduler, frontendClient, _ := setupScheduler(t)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")

	// Stop the scheduler. This will disable receiving new requests from frontends.
	scheduler.StopAsync()

	// We can still send request to scheduler, but we get shutdown error back.
	require.NoError(t, frontendLoop.Send(&FrontendToScheduler{
		Type:        ENQUEUE,
		QueryID:     1,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
	}))

	msg, err := frontendLoop.Recv()
	require.NoError(t, err)
	require.True(t, msg.Status == SHUTTING_DOWN)
}

func TestSchedulerShutdown_QuerierLoop(t *testing.T) {
	scheduler, frontendClient, querierClient := setupScheduler(t)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
	frontendToScheduler(t, frontendLoop, &FrontendToScheduler{
		Type:        ENQUEUE,
		QueryID:     1,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
	})

	// Scheduler now has 1 query. Let's connect querier and fetch it.

	querierLoop, err := querierClient.QuerierLoop(context.Background())
	require.NoError(t, err)
	require.NoError(t, querierLoop.Send(&QuerierToScheduler{QuerierID: "querier-1"}))

	// Dequeue first query.
	_, err = querierLoop.Recv()
	require.NoError(t, err)

	scheduler.StopAsync()

	// Unblock scheduler loop, to find next request.
	err = querierLoop.Send(&QuerierToScheduler{})
	require.NoError(t, err)

	// This should now return with error, since scheduler is going down.
	_, err = querierLoop.Recv()
	require.Error(t, err)
}

func TestSchedulerMaxOutstandingRequests(t *testing.T) {
	_, frontendClient, _ := setupScheduler(t)

	for i := 0; i < testMaxOutstandingPerTenant; i++ {
		// coming from different frontends
		fl := initFrontendLoop(t, frontendClient, fmt.Sprintf("frontend-%d", i))
		require.NoError(t, fl.Send(&FrontendToScheduler{
			Type:        ENQUEUE,
			QueryID:     uint64(i),
			UserID:      "test", // for same user.
			HttpRequest: &httpgrpc.HTTPRequest{},
		}))

		msg, err := fl.Recv()
		require.NoError(t, err)
		require.True(t, msg.Status == OK)
	}

	// One more query from the same user will trigger an error.
	fl := initFrontendLoop(t, frontendClient, "extra-frontend")
	require.NoError(t, fl.Send(&FrontendToScheduler{
		Type:        ENQUEUE,
		QueryID:     0,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
	}))

	msg, err := fl.Recv()
	require.NoError(t, err)
	require.True(t, msg.Status == TOO_MANY_REQUESTS_PER_TENANT)
}

func initFrontendLoop(t *testing.T, client SchedulerForFrontendClient, frontendAddr string) SchedulerForFrontend_FrontendLoopClient {
	loop, err := client.FrontendLoop(context.Background())
	require.NoError(t, err)

	require.NoError(t, loop.Send(&FrontendToScheduler{
		Type:            INIT,
		FrontendAddress: frontendAddr,
	}))

	// Scheduler acks INIT by sending OK back.
	resp, err := loop.Recv()
	require.NoError(t, err)
	require.True(t, resp.Status == OK)

	return loop
}

func frontendToScheduler(t *testing.T, frontendLoop SchedulerForFrontend_FrontendLoopClient, req *FrontendToScheduler) {
	require.NoError(t, frontendLoop.Send(req))
	msg, err := frontendLoop.Recv()
	require.NoError(t, err)
	require.True(t, msg.Status == OK)
}

// If this verification succeeds, there will be leaked goroutine left behind. It will be cleaned once grpc server is shut down.
func verifyQuerierDoesntReceiveRequest(t *testing.T, querierLoop SchedulerForQuerier_QuerierLoopClient, timeout time.Duration) {
	ch := make(chan interface{}, 1)

	go func() {
		m, e := querierLoop.Recv()
		if e != nil {
			ch <- e
		} else {
			ch <- m
		}
	}()

	select {
	case val := <-ch:
		require.Failf(t, "expected timeout", "got %v", val)
	case <-time.After(timeout):
		return
	}
}

func verifyNoPendingRequestsLeft(t *testing.T, scheduler *Scheduler) {
	test.Poll(t, 1*time.Second, 0, func() interface{} {
		scheduler.mtx.Lock()
		defer scheduler.mtx.Unlock()
		return len(scheduler.pendingRequests)
	})
}

type limits struct {
	queriers int
}

func (l limits) MaxQueriersPerUser(_ string) int {
	return l.queriers
}
