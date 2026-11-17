package compactor

import (
	"context"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

type backgrounChunkSeriesSet struct {
	ctx     context.Context
	nextSet chan storage.ChunkSeries
	actual  storage.ChunkSeries
	cs      storage.ChunkSeriesSet
}

func (b *backgrounChunkSeriesSet) Next() bool {
	s, ok := <-b.nextSet
	b.actual = s
	return ok
}

func (b *backgrounChunkSeriesSet) At() storage.ChunkSeries {
	return b.actual
}

func (b *backgrounChunkSeriesSet) Err() error {
	// run returns early on cancellation, so the underlying set can be left
	// unfinished with no error of its own. Report the cancellation so callers
	// do not mistake a partial iteration for a complete one.
	if err := b.ctx.Err(); err != nil {
		return err
	}
	return b.cs.Err()
}

func (b *backgrounChunkSeriesSet) Warnings() annotations.Annotations {
	return b.cs.Warnings()
}

func (b *backgrounChunkSeriesSet) run(ctx context.Context) {
	// Closing on every return path, cancellation included, keeps consumers
	// blocked on Next() from waiting on a channel nobody will ever send to or
	// close again.
	defer close(b.nextSet)
	for {
		if !b.cs.Next() {
			return
		}

		select {
		case b.nextSet <- b.cs.At():
		case <-ctx.Done():
			return
		}
	}
}

// defaultBackgroundChunkSeriesSetBuffer is the lookahead used for a single
// background set. A merge tree holds one buffer per subtree, so inner levels
// pass a smaller value to bound total series held in memory.
const defaultBackgroundChunkSeriesSetBuffer = 1000

func NewBackgroundChunkSeriesSet(ctx context.Context, cs storage.ChunkSeriesSet) storage.ChunkSeriesSet {
	return NewBackgroundChunkSeriesSetWithBuffer(ctx, cs, defaultBackgroundChunkSeriesSetBuffer)
}

func NewBackgroundChunkSeriesSetWithBuffer(ctx context.Context, cs storage.ChunkSeriesSet, buffer int) storage.ChunkSeriesSet {
	if buffer < 1 {
		buffer = 1
	}
	r := &backgrounChunkSeriesSet{
		ctx:     ctx,
		cs:      cs,
		nextSet: make(chan storage.ChunkSeries, buffer),
	}

	go func() {
		r.run(ctx)
	}()

	return r
}
