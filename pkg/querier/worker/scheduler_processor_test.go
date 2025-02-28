package worker

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/cortexproject/cortex/pkg/frontend/v2/frontendv2pb"
	"github.com/cortexproject/cortex/pkg/querier/stats"
	"github.com/cortexproject/cortex/pkg/scheduler/schedulerpb"
)

// mock querier request handler
type mockRequestHandler struct {
	mock.Mock
}

func (m *mockRequestHandler) Handle(ctx context.Context, req *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(*httpgrpc.HTTPResponse), args.Error(1)
}

type mockFrontendForQuerierServer struct {
	mock.Mock
}

func (m *mockFrontendForQuerierServer) QueryResult(_ context.Context, _ *frontendv2pb.QueryResultRequest) (*frontendv2pb.QueryResultResponse, error) {
	return &frontendv2pb.QueryResultResponse{}, nil
}

type mockSchedulerForQuerierClient struct {
	mock.Mock
}

func (m *mockSchedulerForQuerierClient) QuerierLoop(ctx context.Context, opts ...grpc.CallOption) (schedulerpb.SchedulerForQuerier_QuerierLoopClient, error) {
	args := m.Called(ctx, opts)
	return args.Get(0).(schedulerpb.SchedulerForQuerier_QuerierLoopClient), args.Error(1)
}

func (m *mockSchedulerForQuerierClient) NotifyQuerierShutdown(ctx context.Context, in *schedulerpb.NotifyQuerierShutdownRequest, opts ...grpc.CallOption) (*schedulerpb.NotifyQuerierShutdownResponse, error) {
	args := m.Called(ctx, in, opts)
	return args.Get(0).(*schedulerpb.NotifyQuerierShutdownResponse), args.Error(1)
}

// mock SchedulerForQuerier_QuerierLoopClient
type mockQuerierLoopClient struct {
	ctx context.Context
	mock.Mock
}

func (m *mockQuerierLoopClient) Send(msg *schedulerpb.QuerierToScheduler) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *mockQuerierLoopClient) Recv() (*schedulerpb.SchedulerToQuerier, error) {
	args := m.Called()

	if fn, ok := args.Get(0).(func() (*schedulerpb.SchedulerToQuerier, error)); ok {
		return fn()
	}

	return args.Get(0).(*schedulerpb.SchedulerToQuerier), args.Error(1)
}

func (m *mockQuerierLoopClient) Header() (metadata.MD, error) {
	args := m.Called()
	return args.Get(0).(metadata.MD), args.Error(1)
}

func (m *mockQuerierLoopClient) Trailer() metadata.MD {
	args := m.Called()
	return args.Get(0).(metadata.MD)
}

func (m *mockQuerierLoopClient) CloseSend() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockQuerierLoopClient) Context() context.Context {
	args := m.Called()
	return args.Get(0).(context.Context)
}

func (m *mockQuerierLoopClient) SendMsg(msg interface{}) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *mockQuerierLoopClient) RecvMsg(msg interface{}) error {
	args := m.Called(msg)
	return args.Error(0)
}

// To show https://github.com/cortexproject/cortex/issues/6599 issue has been resolved
func Test_ToShowNotPanic_RelatedIssue6599(t *testing.T) {
	cfg := Config{}
	frontendAddress := ":50001"
	userID := "user-1"
	recvCount := 20000

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	recvCall := atomic.Uint32{}

	// mocking query scheduler
	querierLoopClient := &mockQuerierLoopClient{}
	querierLoopClient.ctx = ctx
	querierLoopClient.On("Send", mock.Anything).Return(nil)
	querierLoopClient.On("Context").Return(querierLoopClient.ctx)
	querierLoopClient.On("Recv").Return(func() (*schedulerpb.SchedulerToQuerier, error) {
		recvCall.Add(1)
		if recvCall.Load() <= uint32(recvCount) {
			return &schedulerpb.SchedulerToQuerier{
				QueryID:         1,
				HttpRequest:     &httpgrpc.HTTPRequest{},
				FrontendAddress: frontendAddress,
				UserID:          userID,
				StatsEnabled:    true,
			}, nil
		} else {
			<-querierLoopClient.ctx.Done()
			return nil, context.Canceled
		}

	})

	requestHandler := &mockRequestHandler{}
	requestHandler.On("Handle", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		stat := stats.FromContext(args.Get(0).(context.Context))

		// imitate add query-stat at fetchSeriesFromStores
		go stat.AddFetchedChunkBytes(10)
	}).Return(&httpgrpc.HTTPResponse{}, nil)

	sp, _ := newSchedulerProcessor(cfg, requestHandler, log.NewNopLogger(), nil)
	schedulerClient := &mockSchedulerForQuerierClient{}
	schedulerClient.On("QuerierLoop", mock.Anything, mock.Anything).Return(querierLoopClient, nil)

	sp.schedulerClientFactory = func(conn *grpc.ClientConn) schedulerpb.SchedulerForQuerierClient {
		return schedulerClient
	}

	// frontendForQuerierServer
	grpcServer := grpc.NewServer()
	server := &mockFrontendForQuerierServer{}
	frontendv2pb.RegisterFrontendForQuerierServer(grpcServer, server)

	lis, err := net.Listen("tcp", frontendAddress)
	require.NoError(t, err)
	stopChan := make(chan struct{})
	go func() {
		defer close(stopChan)
		if err := grpcServer.Serve(lis); err != nil {
			return
		}
	}()
	defer func() {
		grpcServer.GracefulStop()
		<-stopChan // Wait util stop complete
	}()

	sp.processQueriesOnSingleStream(ctx, nil, lis.Addr().String())
}
