package compactor

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

// endlessChunkSeriesSet never ends, so the background goroutine always has
// another series to push and parks on the send once the buffer is full.
type endlessChunkSeriesSet struct {
	nextCalls atomic.Int64
}

func (s *endlessChunkSeriesSet) Next() bool {
	s.nextCalls.Add(1)
	return true
}

func (s *endlessChunkSeriesSet) At() storage.ChunkSeries { return nil }

func (s *endlessChunkSeriesSet) Err() error { return nil }

func (s *endlessChunkSeriesSet) Warnings() annotations.Annotations { return nil }

func TestBackgroundChunkSeriesSetStopsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cs := &endlessChunkSeriesSet{}
	set := NewBackgroundChunkSeriesSetWithBuffer(ctx, cs, 1)

	// Wait until the background goroutine has filled the single buffer slot and
	// is parked on the send for the next series. Nothing is consuming yet, so
	// after this the send can not proceed and cancellation is the only way out.
	require.Eventually(t, func() bool {
		return cs.nextCalls.Load() >= 2
	}, 10*time.Second, 10*time.Millisecond, "background goroutine did not start producing")

	cancel()

	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for set.Next() { //nolint:revive // draining until the set reports it is done
		}
	}()

	select {
	case <-drained:
	case <-time.After(10 * time.Second):
		t.Fatal("Next() blocked after the context was cancelled")
	}

	require.ErrorIs(t, set.Err(), context.Canceled)
}
