package chunk

import (
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// Chunk contains encoded timeseries data
type Chunk struct {
	// These fields will be in all chunks, including old ones.
	From    model.Time     `json:"from"`
	Through model.Time     `json:"through"`
	Metric  labels.Labels  `json:"metric"`
	Data    chunkenc.Chunk `json:"-"`
}

// NewChunk creates a new chunk
func NewChunk(metric labels.Labels, c chunkenc.Chunk, from, through model.Time) Chunk {
	return Chunk{
		From:    from,
		Through: through,
		Metric:  metric,
		Data:    c,
	}
}

func (c *Chunk) NewIterator(iterator Iterator) Iterator {
	if c.Data == nil {
		return errorIterator("Prometheus chunk is not set")
	}

	if pit, ok := iterator.(*prometheusChunkIterator); ok {
		pit.c = c.Data
		pit.it = c.Data.Iterator(pit.it)
		return pit
	}

	return &prometheusChunkIterator{c: c.Data, it: c.Data.Iterator(nil)}
}

type prometheusChunkIterator struct {
	c  chunkenc.Chunk // we need chunk, because FindAtOrAfter needs to start with fresh iterator.
	it chunkenc.Iterator
}

func (p *prometheusChunkIterator) Scan() bool {
	return p.it.Next() != chunkenc.ValNone
}

func (p *prometheusChunkIterator) FindAtOrAfter(time model.Time) bool {
	// FindAtOrAfter must return OLDEST value at given time. That means we need to start with a fresh iterator,
	// otherwise we cannot guarantee OLDEST.
	p.it = p.c.Iterator(p.it)
	return p.it.Seek(int64(time)) != chunkenc.ValNone
}

func (p *prometheusChunkIterator) Batch(size int) Batch {
	var batch Batch
	j := 0
	for j < size {
		t, v := p.it.At()
		batch.Timestamps[j] = t
		batch.Values[j] = v
		j++
		if j < size && p.it.Next() == chunkenc.ValNone {
			break
		}
	}
	batch.Index = 0
	batch.Length = j
	return batch
}

func (p *prometheusChunkIterator) Err() error {
	return p.it.Err()
}

type errorIterator string

func (e errorIterator) Scan() bool                         { return false }
func (e errorIterator) FindAtOrAfter(time model.Time) bool { return false }
func (e errorIterator) Batch(size int) Batch               { panic("no values") }
func (e errorIterator) Err() error                         { return errors.New(string(e)) }
