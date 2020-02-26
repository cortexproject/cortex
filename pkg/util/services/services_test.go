package services

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIdleService(t *testing.T) {
	started := false
	stopped := false

	s := NewIdleService(func(ctx context.Context) error {
		started = true
		return nil
	}, func() error {
		stopped = true
		return nil
	})
	defer s.StopAsync()

	require.False(t, started)
	require.Equal(t, New, s.State())
	require.NoError(t, s.StartAsync(context.Background()))
	require.Error(t, s.StartAsync(context.Background())) // cannot start twice
	require.NoError(t, s.AwaitRunning(context.Background()))
	require.True(t, started)
	require.False(t, stopped)
	require.Equal(t, Running, s.State())
	s.StopAsync()
	require.NoError(t, s.AwaitTerminated(context.Background()))
	require.True(t, stopped)
}

func TestTimerService(t *testing.T) {
	iterations := int64(0)

	s := NewTimerService(100*time.Millisecond, nil, func(ctx context.Context) error {
		atomic.AddInt64(&iterations, 1)
		return nil
	}, nil)
	defer s.StopAsync()

	require.Equal(t, New, s.State())
	require.NoError(t, s.StartAsync(context.Background()))
	require.Error(t, s.StartAsync(context.Background()))
	require.NoError(t, s.AwaitRunning(context.Background()))
	require.Equal(t, Running, s.State())

	time.Sleep(1 * time.Second)

	val := atomic.LoadInt64(&iterations)
	require.NotZero(t, val) // we should observe some iterations now

	s.StopAsync()
	require.NoError(t, s.AwaitTerminated(context.Background()))
	require.Equal(t, Terminated, s.State())
}
