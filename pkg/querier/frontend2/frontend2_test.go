package frontend2

import (
	"context"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	"go.uber.org/atomic"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
)

const testFrontendWorkerConcurrency = 5

func setupFrontend2(t *testing.T, schedulerReplyFunc func(f *Frontend2, msg *FrontendToScheduler) *SchedulerToFrontend) (*Frontend2, *mockScheduler) {
	l, err := net.Listen("tcp", "")
	require.NoError(t, err)

	server := grpc.NewServer()

	h, p, err := net.SplitHostPort(l.Addr().String())
	require.NoError(t, err)

	grpcPort, err := strconv.Atoi(p)
	require.NoError(t, err)

	cfg := Config{}
	flagext.DefaultValues(&cfg)
	cfg.SchedulerAddress = l.Addr().String()
	cfg.WorkerConcurrency = testFrontendWorkerConcurrency
	cfg.Addr = h
	cfg.Port = grpcPort

	//logger := log.NewLogfmtLogger(os.Stdout)
	logger := log.NewNopLogger()
	f, err := NewFrontend2(cfg, logger, nil)
	require.NoError(t, err)

	RegisterFrontendForQuerierServer(server, f)

	ms := newMockScheduler(t, f, schedulerReplyFunc)
	RegisterSchedulerForFrontendServer(server, ms)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), f))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), f)
	})

	go func() {
		_ = server.Serve(l)
	}()

	t.Cleanup(func() {
		_ = l.Close()
	})

	// Wait for frontend to connect to scheduler.
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		ms.mu.Lock()
		defer ms.mu.Unlock()

		return len(ms.frontendAddr)
	})

	return f, ms
}

func sendResponseWithDelay(f *Frontend2, delay time.Duration, userID string, queryID uint64, resp *httpgrpc.HTTPResponse) {
	if delay > 0 {
		time.Sleep(delay)
	}

	ctx := user.InjectOrgID(context.Background(), userID)
	_, _ = f.QueryResult(ctx, &QueryResultRequest{
		QueryID:      queryID,
		HttpResponse: resp,
	})
}

func TestFrontendBasicWorkflow(t *testing.T) {
	const (
		body   = "all fine here"
		userID = "test"
	)

	f, _ := setupFrontend2(t, func(f *Frontend2, msg *FrontendToScheduler) *SchedulerToFrontend {
		// We cannot call QueryResult directly, as Frontend is not yet waiting for the response.
		// It first needs to be told that enqueuing has succeeded.
		go sendResponseWithDelay(f, 100*time.Millisecond, userID, msg.QueryID, &httpgrpc.HTTPResponse{
			Code: 200,
			Body: []byte(body),
		})

		return &SchedulerToFrontend{Status: OK}
	})

	resp, err := f.RoundTripGRPC(user.InjectOrgID(context.Background(), userID), &httpgrpc.HTTPRequest{})
	require.NoError(t, err)
	require.Equal(t, int32(200), resp.Code)
	require.Equal(t, []byte(body), resp.Body)
}

func TestFrontendRetryEnqueue(t *testing.T) {
	// Frontend uses worker concurrency to compute number of retries. We use one less failure.
	failures := atomic.NewInt64(testFrontendWorkerConcurrency - 1)
	const (
		body   = "hello world"
		userID = "test"
	)

	f, _ := setupFrontend2(t, func(f *Frontend2, msg *FrontendToScheduler) *SchedulerToFrontend {
		fail := failures.Dec()
		if fail >= 0 {
			return &SchedulerToFrontend{Status: SHUTTING_DOWN}
		}

		go sendResponseWithDelay(f, 100*time.Millisecond, userID, msg.QueryID, &httpgrpc.HTTPResponse{
			Code: 200,
			Body: []byte(body),
		})

		return &SchedulerToFrontend{Status: OK}
	})

	_, err := f.RoundTripGRPC(user.InjectOrgID(context.Background(), userID), &httpgrpc.HTTPRequest{})
	require.NoError(t, err)
}

func TestFrontendEnqueueFailure(t *testing.T) {
	f, _ := setupFrontend2(t, func(f *Frontend2, msg *FrontendToScheduler) *SchedulerToFrontend {
		return &SchedulerToFrontend{Status: SHUTTING_DOWN}
	})

	_, err := f.RoundTripGRPC(user.InjectOrgID(context.Background(), "test"), &httpgrpc.HTTPRequest{})
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "failed to enqueue request"))
}

func TestFrontendCancellation(t *testing.T) {
	f, ms := setupFrontend2(t, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	resp, err := f.RoundTripGRPC(user.InjectOrgID(ctx, "test"), &httpgrpc.HTTPRequest{})
	require.EqualError(t, err, context.DeadlineExceeded.Error())
	require.Nil(t, resp)

	// We wait a bit to make sure scheduler receives the cancellation request.
	test.Poll(t, time.Second, 2, func() interface{} {
		ms.mu.Lock()
		defer ms.mu.Unlock()

		return len(ms.msgs)
	})

	ms.checkWithLock(func() {
		require.Equal(t, 2, len(ms.msgs))
		require.True(t, ms.msgs[0].Type == ENQUEUE)
		require.True(t, ms.msgs[1].Type == CANCEL)
		require.True(t, ms.msgs[0].QueryID == ms.msgs[1].QueryID)
	})
}

func TestFrontendFailedCancellation(t *testing.T) {
	f, ms := setupFrontend2(t, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		time.Sleep(100 * time.Millisecond)

		// stop scheduler workers
		addr := ""
		f.schedulerWorkers.mu.Lock()
		for k := range f.schedulerWorkers.workers {
			addr = k
			break
		}
		f.schedulerWorkers.mu.Unlock()

		f.schedulerWorkers.AddressRemoved(addr)

		// Wait for worker goroutines to stop.
		time.Sleep(100 * time.Millisecond)

		// Cancel request. Frontend will try to send cancellation to scheduler, but that will fail (not visible to user).
		// Everything else should still work fine.
		cancel()
	}()

	// send request
	resp, err := f.RoundTripGRPC(user.InjectOrgID(ctx, "test"), &httpgrpc.HTTPRequest{})
	require.EqualError(t, err, context.Canceled.Error())
	require.Nil(t, resp)

	ms.checkWithLock(func() {
		require.Equal(t, 1, len(ms.msgs))
	})
}

type mockScheduler struct {
	t *testing.T
	f *Frontend2

	replyFunc func(f *Frontend2, msg *FrontendToScheduler) *SchedulerToFrontend

	mu           sync.Mutex
	frontendAddr map[string]int
	msgs         []*FrontendToScheduler
}

func newMockScheduler(t *testing.T, f *Frontend2, replyFunc func(f *Frontend2, msg *FrontendToScheduler) *SchedulerToFrontend) *mockScheduler {
	return &mockScheduler{t: t, f: f, frontendAddr: map[string]int{}, replyFunc: replyFunc}
}

func (m *mockScheduler) checkWithLock(fn func()) {
	m.mu.Lock()
	defer m.mu.Unlock()

	fn()
}

func (m *mockScheduler) FrontendLoop(frontend SchedulerForFrontend_FrontendLoopServer) error {
	init, err := frontend.Recv()
	if err != nil {
		return err
	}

	m.mu.Lock()
	m.frontendAddr[init.FrontendAddress]++
	m.mu.Unlock()

	// Ack INIT from frontend.
	if err := frontend.Send(&SchedulerToFrontend{Status: OK}); err != nil {
		return err
	}

	for {
		msg, err := frontend.Recv()
		if err != nil {
			return err
		}

		m.mu.Lock()
		m.msgs = append(m.msgs, msg)
		m.mu.Unlock()

		reply := &SchedulerToFrontend{Status: OK}
		if m.replyFunc != nil {
			reply = m.replyFunc(m.f, msg)
		}

		if err := frontend.Send(reply); err != nil {
			return err
		}
	}
}
