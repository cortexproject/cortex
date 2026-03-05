package ruler

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/cortexpb"
)

type mockPusher struct {
	lastRequest *cortexpb.WriteRequest
	pushError   error
}

func (m *mockPusher) Push(ctx context.Context, req *cortexpb.WriteRequest) (*cortexpb.WriteResponse, error) {
	m.lastRequest = req
	return &cortexpb.WriteResponse{}, m.pushError
}

func TestPusherAppender_Commit_WithDiscardOutOfOrder(t *testing.T) {
	mock := &mockPusher{}
	counter := prometheus.NewCounter(prometheus.CounterOpts{Name: "test"})

	appender := &PusherAppender{
		ctx:          context.Background(),
		pusher:       mock,
		userID:       "test-user",
		totalWrites:  counter,
		failedWrites: counter,
		labels:       []labels.Labels{labels.FromStrings("__name__", "test_metric")},
		samples:      []cortexpb.Sample{{TimestampMs: 1000, Value: 1.0}},
	}

	appender.SetOptions(&storage.AppendOptions{DiscardOutOfOrder: true})

	err := appender.Commit()
	require.NoError(t, err)

	// Verify that DiscardOutOfOrder was set in the WriteRequest
	require.NotNil(t, mock.lastRequest, "WriteRequest should have been sent")
	assert.True(t, mock.lastRequest.DiscardOutOfOrder, "DiscardOutOfOrder should be true in WriteRequest")
}

func TestPusherAppender_Commit_WithoutDiscardOutOfOrder(t *testing.T) {
	mock := &mockPusher{}
	counter := prometheus.NewCounter(prometheus.CounterOpts{Name: "test"})

	appender := &PusherAppender{
		ctx:          context.Background(),
		pusher:       mock,
		userID:       "test-user",
		totalWrites:  counter,
		failedWrites: counter,
		labels:       []labels.Labels{labels.FromStrings("__name__", "test_metric")},
		samples:      []cortexpb.Sample{{TimestampMs: 1000, Value: 1.0}},
	}

	appender.SetOptions(&storage.AppendOptions{DiscardOutOfOrder: false})

	err := appender.Commit()
	require.NoError(t, err)

	require.NotNil(t, mock.lastRequest, "WriteRequest should have been sent")
	assert.False(t, mock.lastRequest.DiscardOutOfOrder, "DiscardOutOfOrder should be false in WriteRequest")
}

func TestPusherAppender_Commit_WithNilOptions(t *testing.T) {
	mock := &mockPusher{}
	counter := prometheus.NewCounter(prometheus.CounterOpts{Name: "test"})

	appender := &PusherAppender{
		ctx:          context.Background(),
		pusher:       mock,
		userID:       "test-user",
		totalWrites:  counter,
		failedWrites: counter,
		labels:       []labels.Labels{labels.FromStrings("__name__", "test_metric")},
		samples:      []cortexpb.Sample{{TimestampMs: 1000, Value: 1.0}},
		opts:         nil, // Explicitly nil
	}

	err := appender.Commit()
	require.NoError(t, err)

	require.NotNil(t, mock.lastRequest, "WriteRequest should have been sent")
	assert.False(t, mock.lastRequest.DiscardOutOfOrder, "DiscardOutOfOrder should be false when opts is nil")
}
