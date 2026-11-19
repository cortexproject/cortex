package compactor

import (
	"context"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

type backgrounChunkSeriesSet struct {
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
	return b.cs.Err()
}

func (b *backgrounChunkSeriesSet) Warnings() annotations.Annotations {
	return b.cs.Warnings()
}

func (b *backgrounChunkSeriesSet) run(ctx context.Context) {
	for {
		if !b.cs.Next() {
			close(b.nextSet)
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
		cs:      cs,
		nextSet: make(chan storage.ChunkSeries, buffer),
	}

	go func() {
		r.run(ctx)
	}()

	return r
}
