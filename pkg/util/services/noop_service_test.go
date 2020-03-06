package services

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNoopService(t *testing.T) {
	l := newServiceListener()
	require.NoError(t, StartAndAwaitRunning(context.Background(), l))

	n := NewNoopService()
	n.AddListener(l)

	require.NoError(t, n.StartAsync(context.Background()))
	require.Error(t, n.StartAsync(context.Background()))

	require.NoError(t, n.AwaitRunning(context.Background()))
	require.NoError(t, n.AwaitRunning(context.Background()))

	// calling it multiple times should work fine
	n.StopAsync()
	n.StopAsync()
	n.StopAsync()

	require.NoError(t, n.AwaitTerminated(context.Background()))
	require.NoError(t, n.FailureCase())

	require.NoError(t, StopAndAwaitTerminated(context.Background(), l))
	require.Equal(t, []string{"starting", "running", "stopping: Running", "terminated: Stopping"}, l.log)
}

func TestNoopServiceNewToTerminated(t *testing.T) {
	l := newServiceListener()
	require.NoError(t, StartAndAwaitRunning(context.Background(), l))

	n := NewNoopService()
	n.AddListener(l)

	require.NoError(t, StopAndAwaitTerminated(context.Background(), n))
	n.StopAsync()
	require.NoError(t, n.FailureCase())

	require.NoError(t, StopAndAwaitTerminated(context.Background(), l))
	require.Equal(t, []string{"terminated: New"}, l.log)
}
