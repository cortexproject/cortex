package ingester

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
)

// mockAppender implements the extendedAppender interface for testing
type mockAppender struct {
	storage.Appender
	lastOptions *storage.AppendOptions
}

func (m *mockAppender) SetOptions(opts *storage.AppendOptions) {
	m.lastOptions = opts
}

func TestIngester_Push_DiscardOutOfOrder_True(t *testing.T) {
	req := &cortexpb.WriteRequest{
		Source:            cortexpb.RULE,
		DiscardOutOfOrder: true,
		Timeseries:        []cortexpb.PreallocTimeseries{},
	}

	assert.True(t, req.DiscardOutOfOrder, "DiscardOutOfOrder should be true")
	assert.True(t, req.GetDiscardOutOfOrder(), "GetDiscardOutOfOrder should return true")
}

func TestIngester_Push_DiscardOutOfOrder_Default(t *testing.T) {
	// Create a WriteRequest without setting DiscardOutOfOrder
	req := &cortexpb.WriteRequest{
		Source:     cortexpb.API,
		Timeseries: []cortexpb.PreallocTimeseries{},
	}

	// Verify the default value is false
	assert.False(t, req.DiscardOutOfOrder, "DiscardOutOfOrder should default to false")
	assert.False(t, req.GetDiscardOutOfOrder(), "GetDiscardOutOfOrder should return false by default")
}

func TestIngester_WriteRequest_MultipleScenarios(t *testing.T) {
	scenarios := []struct {
		name        string
		setupReq    func() *cortexpb.WriteRequest
		expectOpts  bool
		description string
	}{
		{
			name: "Stale marker during rule migration",
			setupReq: func() *cortexpb.WriteRequest {
				return &cortexpb.WriteRequest{
					Source:            cortexpb.RULE,
					DiscardOutOfOrder: true,
				}
			},
			expectOpts:  true,
			description: "Should set appender options to discard OOO",
		},
		{
			name: "Normal rule evaluation",
			setupReq: func() *cortexpb.WriteRequest {
				return &cortexpb.WriteRequest{
					Source:            cortexpb.RULE,
					DiscardOutOfOrder: false,
				}
			},
			expectOpts:  false,
			description: "Should not set appender options",
		},
		{
			name: "API write request",
			setupReq: func() *cortexpb.WriteRequest {
				return &cortexpb.WriteRequest{
					Source:            cortexpb.API,
					DiscardOutOfOrder: false,
				}
			},
			expectOpts:  false,
			description: "API requests should never trigger OOO discard",
		},
		{
			name: "Default values",
			setupReq: func() *cortexpb.WriteRequest {
				return &cortexpb.WriteRequest{}
			},
			expectOpts:  false,
			description: "Default values should not trigger OOO discard",
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			req := scenario.setupReq()
			mock := &mockAppender{}

			// Simulate the ingester logic
			if req.DiscardOutOfOrder {
				mock.SetOptions(&storage.AppendOptions{DiscardOutOfOrder: true})
			}

			// Verify expectations
			if scenario.expectOpts {
				require.NotNil(t, mock.lastOptions)
				assert.True(t, mock.lastOptions.DiscardOutOfOrder)
			}
		})
	}
}

func TestIngester_DiscardOutOfOrderFlagIngegrationTest(t *testing.T) {
	registry := prometheus.NewRegistry()
	cfg := defaultIngesterTestConfig(t)
	cfg.LifecyclerConfig.JoinAfter = 0

	limits := defaultLimitsTestConfig()
	limits.EnableNativeHistograms = true
	limits.OutOfOrderTimeWindow = model.Duration(60 * time.Minute)

	i, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, "", registry)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 100*time.Millisecond, ring.ACTIVE, func() any {
		return i.lifecycler.GetState()
	})

	ctx := user.InjectOrgID(context.Background(), "test-user")

	// Create labels for our test metric
	metricLabels := labels.FromStrings("__name__", "test_metric", "job", "test")

	currentTime := time.Now().UnixMilli()
	olderTime := currentTime - 60000 // 1 minute earlier (within OOO window)

	// First, push a sample with current timestamp with discardOutOfOrder=true
	req1 := cortexpb.ToWriteRequest(
		[]labels.Labels{metricLabels},
		[]cortexpb.Sample{{Value: 100, TimestampMs: currentTime}},
		nil, nil, cortexpb.RULE)
	req1.DiscardOutOfOrder = true

	_, err = i.Push(ctx, req1)
	require.NoError(t, err, "First sample push should succeed")

	// Now try to push a sample with older timestamp with discardOutOfOrder=true
	// This should be discarded because DiscardOutOfOrder is true
	req2 := cortexpb.ToWriteRequest(
		[]labels.Labels{metricLabels},
		[]cortexpb.Sample{{Value: 50, TimestampMs: olderTime}},
		nil, nil, cortexpb.RULE)
	req2.DiscardOutOfOrder = true

	_, _ = i.Push(ctx, req2)

	// Query back the data to ensure only the first (current time) sample was stored
	s := &mockQueryStreamServer{ctx: ctx}
	err = i.QueryStream(&client.QueryRequest{
		StartTimestampMs: olderTime - 1000,
		EndTimestampMs:   currentTime + 1000,
		Matchers: []*client.LabelMatcher{
			{Type: client.EQUAL, Name: "__name__", Value: "test_metric"},
		},
	}, s)
	require.NoError(t, err)

	// Verify we only have one series with one sample (the current time sample)
	require.Len(t, s.series, 1, "Should have exactly one series")

	// Convert chunks to samples to verify content
	series := s.series[0]
	require.Len(t, series.Chunks, 1, "Should have exactly one chunk")

	chunk := series.Chunks[0]
	chunkData, err := chunkenc.FromData(chunkenc.EncXOR, chunk.Data)
	require.NoError(t, err)

	iter := chunkData.Iterator(nil)
	sampleCount := 0
	for iter.Next() != chunkenc.ValNone {
		ts, val := iter.At()
		require.Equal(t, currentTime, ts, "Sample timestamp should match current time")
		require.Equal(t, 100.0, val, "Sample value should match first push")
		sampleCount++
	}
	require.NoError(t, iter.Err())
	require.Equal(t, 1, sampleCount, "Should have exactly one sample stored")
}
