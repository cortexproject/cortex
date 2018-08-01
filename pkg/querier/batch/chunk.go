package batch

import (
	"github.com/prometheus/common/model"
	promchunk "github.com/weaveworks/cortex/pkg/prom1/storage/local/chunk"

	"github.com/weaveworks/cortex/pkg/chunk"
)

// chunkIterator implement batchIterator over a chunk.
type chunkIterator struct {
	chunk chunk.Chunk
	it    promchunk.Iterator
	batch batch
}

func newChunkIterator(chunk chunk.Chunk) *chunkIterator {
	return &chunkIterator{
		chunk: chunk,
		it:    chunk.Data.NewIterator(),
	}
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
	return i.batch.length > 0
}

func (i *chunkIterator) buildNextBatch() {
	j := 0
	for ; j < batchSize && i.it.Scan(); j++ {
		v := i.it.Value()
		i.batch.timestamps[j] = int64(v.Timestamp)
		i.batch.values[j] = float64(v.Value)
	}
	i.batch.index = 0
	i.batch.length = j
}

func (i *chunkIterator) AtTime() int64 {
	return i.batch.timestamps[0]
}

func (i *chunkIterator) Batch() batch {
	return i.batch
}

func (i *chunkIterator) Err() error {
	return i.it.Err()
}
