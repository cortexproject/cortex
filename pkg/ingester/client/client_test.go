package client

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/util"
)

// TestMarshall is useful to try out various optimisation on the unmarshalling code.
func TestMarshall(t *testing.T) {
	const numSeries = 10
	recorder := httptest.NewRecorder()
	{
		req := cortexpb.WriteRequest{}
		for i := range numSeries {
			req.Timeseries = append(req.Timeseries, cortexpb.PreallocTimeseries{
				TimeSeries: &cortexpb.TimeSeries{
					Labels: []cortexpb.LabelAdapter{
						{Name: "foo", Value: strconv.Itoa(i)},
					},
					Samples: []cortexpb.Sample{
						{TimestampMs: int64(i), Value: float64(i)},
					},
				},
			})
		}
		err := util.SerializeProtoResponse(recorder, &req, util.RawSnappy)
		require.NoError(t, err)
	}

	{
		const (
			tooSmallSize = 1
			plentySize   = 1024 * 1024
		)
		req := cortexpb.WriteRequest{}
		err := util.ParseProtoReader(context.Background(), recorder.Body, recorder.Body.Len(), tooSmallSize, &req, util.RawSnappy)
		require.Error(t, err)
		err = util.ParseProtoReader(context.Background(), recorder.Body, recorder.Body.Len(), plentySize, &req, util.RawSnappy)
		require.NoError(t, err)
		require.Equal(t, numSeries, len(req.Timeseries))
	}
}

func TestClosableHealthAndIngesterClient_MaxInflightPushRequests(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		inflightPushRequests    int64
		maxInflightPushRequests int64
		expectThrottle          bool
	}{
		"no limit": {
			inflightPushRequests:    1000,
			maxInflightPushRequests: 0,
			expectThrottle:          false,
		},
		"inflight request is under limit": {
			inflightPushRequests:    99,
			maxInflightPushRequests: 100,
			expectThrottle:          false,
		},
		"inflight request hits limit": {
			inflightPushRequests:    100,
			maxInflightPushRequests: 100,
			expectThrottle:          true,
		},
	}
	ctx := context.Background()
	for testName, testData := range tests {
		tData := testData
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			client1 := createTestIngesterClient(tData.maxInflightPushRequests, tData.inflightPushRequests)
			_, err := client1.Push(ctx, nil)
			if tData.expectThrottle {
				assert.ErrorIs(t, err, errTooManyInflightPushRequests)
			} else {
				assert.NoError(t, err)
			}

			client2 := createTestIngesterClient(tData.maxInflightPushRequests, tData.inflightPushRequests)
			_, err = client2.PushPreAlloc(ctx, nil)
			if tData.expectThrottle {
				assert.ErrorIs(t, err, errTooManyInflightPushRequests)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func createTestIngesterClient(maxInflightPushRequests int64, currentInflightRequests int64) *closableHealthAndIngesterClient {
	client := &closableHealthAndIngesterClient{
		IngesterClient:          &mockIngester{},
		conn:                    &mockClientConn{},
		addr:                    "dummy_addr",
		maxInflightPushRequests: maxInflightPushRequests,
		inflightPushRequests:    prometheus.NewGaugeVec(prometheus.GaugeOpts{}, []string{"ingester"}),
	}
	client.inflightRequests.Add(currentInflightRequests)
	return client
}

type mockIngester struct {
	IngesterClient
	mock.Mock
}

func (m *mockIngester) Push(_ context.Context, _ *cortexpb.WriteRequest, _ ...grpc.CallOption) (*cortexpb.WriteResponse, error) {
	return &cortexpb.WriteResponse{}, nil
}

func (m *mockIngester) PushStream(ctx context.Context, opts ...grpc.CallOption) (Ingester_PushStreamClient, error) {
	args := m.Called(ctx, opts)
	return args.Get(0).(Ingester_PushStreamClient), nil
}

type mockClientConn struct {
	ClosableClientConn
}

func (m *mockClientConn) Invoke(_ context.Context, _ string, _ any, _ any, _ ...grpc.CallOption) error {
	return nil
}

func (m *mockClientConn) Close() error {
	return nil
}

func TestClosableHealthAndIngesterClient_Close_Basic(t *testing.T) {
	client := &closableHealthAndIngesterClient{
		conn:                 &mockClientConn{},
		addr:                 "test-addr",
		inflightPushRequests: prometheus.NewGaugeVec(prometheus.GaugeOpts{}, []string{"ingester"}),
	}

	err := client.Close()
	assert.NoError(t, err)
}

func TestClosableHealthAndIngesterClient_Close_WithActiveStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	streamChan := make(chan *streamWriteJob, 1)

	job := &streamWriteJob{
		sendDone: make(chan struct{}),
	}
	streamChan <- job

	client := &closableHealthAndIngesterClient{
		conn:                 &mockClientConn{},
		addr:                 "test-addr",
		inflightPushRequests: prometheus.NewGaugeVec(prometheus.GaugeOpts{}, []string{"ingester"}),
		streamCtx:            ctx,
		streamCancel:         cancel,
		streamPushChan:       streamChan,
	}

	err := client.Close()
	assert.NoError(t, err)

	// Verify stream channel is closed
	_, ok := <-client.streamPushChan
	assert.False(t, ok, "stream channel should be closed")

	// Verify job.sendDone was closed by Close()
	select {
	case <-job.sendDone:
		// Success - sendDone was closed
	case <-time.After(100 * time.Millisecond):
		t.Error("job.sendDone was not closed")
	}

	// Verify context is cancelled
	select {
	case <-client.streamCtx.Done():
		// Success - context was cancelled
	case <-time.After(100 * time.Millisecond):
		t.Error("stream context was not cancelled")
	}
}

func TestClosableHealthAndIngesterClient_Close_WithPendingJobs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	streamChan := make(chan *streamWriteJob, 2)

	job1Done := make(chan struct{})
	job2Done := make(chan struct{})

	job1 := &streamWriteJob{sendDone: job1Done}
	job2 := &streamWriteJob{sendDone: job2Done}
	streamChan <- job1
	streamChan <- job2

	client := &closableHealthAndIngesterClient{
		conn:                 &mockClientConn{},
		addr:                 "test-addr",
		inflightPushRequests: prometheus.NewGaugeVec(prometheus.GaugeOpts{}, []string{"ingester"}),
		streamCtx:            ctx,
		streamCancel:         cancel,
		streamPushChan:       streamChan,
	}

	err := client.Close()
	assert.NoError(t, err)

	_, ok := <-client.streamPushChan
	assert.False(t, ok, "stream channel should be closed")

	select {
	case <-client.streamCtx.Done():
	case <-time.After(500 * time.Millisecond):
		t.Error("stream context was not cancelled")
	}

	// Verify jobs were closed (sendDone channels closed)
	select {
	case <-job1Done:
	case <-time.After(500 * time.Millisecond):
		t.Error("job1.sendDone was not closed")
	}
	select {
	case <-job2Done:
	case <-time.After(500 * time.Millisecond):
		t.Error("job2.sendDone was not closed")
	}
}

type mockClientStream struct {
	mock.Mock
	grpc.ClientStream
}

func (m *mockClientStream) Send(msg *cortexpb.StreamWriteRequest) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *mockClientStream) Recv() (*cortexpb.WriteResponse, error) {
	return &cortexpb.WriteResponse{}, nil
}

// slowSendStream simulates a slow gRPC stream.
// Send() pre-computes the buffer size (mirroring gRPC codec step 1),
// sleeps for sendDelay so the caller's context deadline can fire first,
// then calls MarshalToSizedBuffer (gRPC codec step 2).
type slowSendStream struct {
	grpc.ClientStream
	sendDelay time.Duration
	panicCh   chan any
}

func (s *slowSendStream) Send(req *cortexpb.StreamWriteRequest) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			s.panicCh <- r // forward the panic value to the test
		} else {
			s.panicCh <- nil
		}
	}()

	// gRPC codec pre-computes buffer size.
	size := req.Size()
	buf := make([]byte, size)

	// Sleep so the caller's ctx deadline fires and PushStreamConnection returns.
	// After the sleep the caller may have grown the timeseries.
	time.Sleep(s.sendDelay)

	// marshal into the pre-allocated buffer.
	// Panics when actual data > size (the bug).
	_, err := req.MarshalToSizedBuffer(buf)
	return err
}

func (s *slowSendStream) Recv() (*cortexpb.WriteResponse, error) {
	return &cortexpb.WriteResponse{}, nil
}

// TestPushStreamConnection_PanicWhenCtxExpiresAndTimeseriesGrows is an
// end-to-end regression test for the distributor panic.
func TestPushStreamConnection_PanicWhenCtxExpiresAndTimeseriesGrows(t *testing.T) {
	const (
		// ctxDeadline mirrors distributor.remote-timeout.
		// Kept short here so the test completes quickly.
		ctxDeadline = 20 * time.Millisecond
		// sendDelay must exceed ctxDeadline so the deadline fires while Send()
		// is still sleeping between Size() and MarshalToSizedBuffer().
		sendDelay = 200 * time.Millisecond
	)

	ts := cortexpb.TimeseriesFromPool()
	ts.Labels = append(ts.Labels,
		cortexpb.LabelAdapter{Name: "__name__", Value: "test_metric"},
		cortexpb.LabelAdapter{Name: "job", Value: "test"},
	)
	ts.Samples = append(ts.Samples, cortexpb.Sample{Value: 1.0, TimestampMs: 1000})

	timeseries := cortexpb.PreallocTimeseriesSliceFromPool()
	timeseries = append(timeseries, cortexpb.PreallocTimeseries{TimeSeries: ts})

	writeReq := &cortexpb.WriteRequest{Timeseries: timeseries}

	panicCh := make(chan any, 1)
	stream := &slowSendStream{
		sendDelay: sendDelay,
		panicCh:   panicCh,
	}

	mockIng := &mockIngester{}
	mockIng.On("PushStream", mock.Anything, mock.Anything).Return(stream, nil)

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	client := &closableHealthAndIngesterClient{
		IngesterClient:       mockIng,
		conn:                 &mockClientConn{},
		addr:                 "test-addr",
		inflightPushRequests: prometheus.NewGaugeVec(prometheus.GaugeOpts{}, []string{"ingester"}),
		streamCtx:            streamCtx,
		streamCancel:         streamCancel,
		streamPushChan:       make(chan *streamWriteJob, 1),
	}

	workerCtx := user.InjectOrgID(streamCtx, "test-worker")
	require.NoError(t, client.worker(workerCtx))

	// Call PushStreamConnection with a context that expires before Send() finishes.
	pushCtx, pushCancel := context.WithTimeout(
		user.InjectOrgID(context.Background(), "test-tenant"),
		ctxDeadline,
	)
	defer pushCancel()

	// PushStreamConnection blocks until Send()+Recv() complete.
	_, pushErr := client.PushStreamConnection(pushCtx, writeReq)
	require.ErrorIs(t, pushErr, context.DeadlineExceeded,
		"caller should observe its own context deadline")

	for i := range 100 {
		ts.Labels = append(ts.Labels, cortexpb.LabelAdapter{
			Name:  fmt.Sprintf("extra_label_%d", i),
			Value: fmt.Sprintf("extra_value_%d", i),
		})
	}

	// No panic expected: Send() already completed before labels were appended.
	select {
	case panicVal := <-panicCh:
		require.Nil(t, panicVal,
			"unexpected panic in MarshalToSizedBuffer: the fix should prevent "+
				"timeseries from being reused while Send() is still marshalling")
	case <-time.After(sendDelay + time.Second):
		t.Fatal("timed out waiting for Send() to complete")
	}
}

func TestClosableHealthAndIngesterClient_ShouldNotPanicWhenClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	streamChan := make(chan *streamWriteJob)

	mockIngester := &mockIngester{}
	mockStream := &mockClientStream{}
	mockIngester.On("PushStream", mock.Anything, mock.Anything).Return(mockStream, nil).Once()

	client := &closableHealthAndIngesterClient{
		IngesterClient:       mockIngester,
		conn:                 &mockClientConn{},
		addr:                 "test-addr",
		inflightPushRequests: prometheus.NewGaugeVec(prometheus.GaugeOpts{}, []string{"ingester"}),
		streamCtx:            ctx,
		streamCancel:         cancel,
		streamPushChan:       streamChan,
	}
	require.NoError(t, client.worker(context.Background()))
	require.NoError(t, client.Close())

	time.Sleep(100 * time.Millisecond)
}
