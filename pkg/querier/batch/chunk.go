package batch

import (
	"github.com/prometheus/common/model"
	"github.com/weaveworks/cortex/pkg/chunk"
	promchunk "github.com/weaveworks/cortex/pkg/prom1/storage/local/chunk"
)

// chunkIterator implement batchIterator over a chunk.
type chunkIterator struct {
	chunk chunk.Chunk
	it    promchunk.Iterator
	batch promchunk.Batch
}

func (i *chunkIterator) reset(chunk chunk.Chunk) {
	i.chunk = chunk
	i.it = chunk.Data.NewIterator()
	i.batch.Length = 0
	i.batch.Index = 0
}

// Seek advances the iterator forward to the value at or after
// the given timestamp.
func (i *chunkIterator) Seek(t int64) bool {
	// We assume seeks only care about a specific window; if this chunk doesn't
	// contain samples in that window, we can shortcut.
	if int64(i.chunk.Through) < t {
		return false
	}

	// TODO this will call next twice.
	next := i.it.FindAtOrAfter(model.Time(t))
	if next {
		i.buildNextBatch()
	}
	return next
}

func (i *chunkIterator) Next() bool {
	i.buildNextBatch()
	return i.batch.Length > 0
}

func (i *chunkIterator) buildNextBatch() {
	i.batch = i.it.Batch()
}

func (i *chunkIterator) AtTime() int64 {
	return i.batch.Timestamps[0]
}

func (i *chunkIterator) Batch() promchunk.Batch {
	return i.batch
}

func (i *chunkIterator) Err() error {
	return i.it.Err()
}
