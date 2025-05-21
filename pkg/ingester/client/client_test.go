package client

import (
	"context"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		for i := 0; i < numSeries; i++ {
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
}

func (m *mockIngester) Push(_ context.Context, _ *cortexpb.WriteRequest, _ ...grpc.CallOption) (*cortexpb.WriteResponse, error) {
	return &cortexpb.WriteResponse{}, nil
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

	jobCtx, jobCancel := context.WithCancel(context.Background())
	job := &streamWriteJob{
		ctx:    jobCtx,
		cancel: jobCancel,
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

	job1Cancelled := false
	job2Cancelled := false

	job1 := &streamWriteJob{
		ctx: context.Background(),
		cancel: func() {
			job1Cancelled = true
		},
	}
	job2 := &streamWriteJob{
		ctx: context.Background(),
		cancel: func() {
			job2Cancelled = true
		},
	}
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

	// Verify jobs were cancelled
	assert.True(t, job1Cancelled, "job1 should have been cancelled")
	assert.True(t, job2Cancelled, "job2 should have been cancelled")
}
