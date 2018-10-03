package batch

import (
	"github.com/cortexproject/cortex/pkg/chunk"
	promchunk "github.com/cortexproject/cortex/pkg/prom1/storage/local/chunk"
	"github.com/prometheus/common/model"
)

// chunkIterator implement batchIterator over a chunk.  Its is designed to be
// reused by calling reset() with a fresh chunk.
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
func (i *chunkIterator) Seek(t int64, size int) bool {
	// We assume seeks only care about a specific window; if this chunk doesn't
	// contain samples in that window, we can shortcut.
	if int64(i.chunk.Through) < t {
		return false
	}

	if i.it.FindAtOrAfter(model.Time(t)) {
		i.batch = i.it.Batch(size)
		return i.batch.Length > 0
	}
	return false
}

func (i *chunkIterator) Next(size int) bool {
	if i.it.Scan() {
		i.batch = i.it.Batch(size)
		return i.batch.Length > 0
	}
	return false
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
