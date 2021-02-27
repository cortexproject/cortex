package encoding

import (
	"io"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// Read-only wrapper around Prometheus chunk.
type prometheusXorChunk struct {
	chunk chunkenc.Chunk
}

func newPrometheusXorChunk() *prometheusXorChunk {
	return &prometheusXorChunk{}
}

func (p *prometheusXorChunk) Add(_ model.SamplePair) (Chunk, error) {
	return nil, errors.New("cannot add new samples to Prometheus chunk")
}

func (p *prometheusXorChunk) NewIterator(iterator Iterator) Iterator {
	if p.chunk == nil {
		// TODO: return error iterator
		return nil
	}

	if pit, ok := iterator.(*prometheusChunkIterator); ok {
		pit.it = p.chunk.Iterator(pit.it)
		return pit
	}

	return &prometheusChunkIterator{p.chunk.Iterator(nil)}
}

func (p *prometheusXorChunk) Marshal(i io.Writer) error {
	if p.chunk == nil {
		return errors.New("chunk data not set")
	}
	_, err := i.Write(p.chunk.Bytes())
	return err
}

func (p *prometheusXorChunk) UnmarshalFromBuf(bytes []byte) error {
	c, err := chunkenc.FromData(chunkenc.EncXOR, bytes)
	if err != nil {
		return errors.Wrap(err, "failed to create Prometheus chunk from bytes")
	}

	p.chunk = c
	return nil
}

func (p *prometheusXorChunk) Encoding() Encoding {
	return PrometheusXorChunk
}

func (p *prometheusXorChunk) Utilization() float64 {
	// Used for reporting when chunk is used to store new data.
	return 0
}

func (p *prometheusXorChunk) Slice(_, _ model.Time) Chunk {
	// Not implemented.
	return p
}

func (p *prometheusXorChunk) Rebound(_, _ model.Time) (Chunk, error) {
	return nil, errors.New("rebound not supported by Prometheus chunk")
}

func (p *prometheusXorChunk) Len() int {
	return p.Size()
}

func (p *prometheusXorChunk) Size() int {
	if p.chunk == nil {
		return 0
	}
	return len(p.chunk.Bytes())
}

type prometheusChunkIterator struct {
	it chunkenc.Iterator
}

func (p *prometheusChunkIterator) Scan() bool {
	return p.it.Next()
}

func (p *prometheusChunkIterator) FindAtOrAfter(time model.Time) bool {
	return p.it.Seek(int64(time))
}

func (p *prometheusChunkIterator) Value() model.SamplePair {
	ts, val := p.it.At()
	return model.SamplePair{
		Timestamp: model.Time(ts),
		Value:     model.SampleValue(val),
	}
}

func (p *prometheusChunkIterator) Batch(size int) Batch {
	var batch Batch
	j := 0
	for j < size {
		t, v := p.it.At()
		batch.Timestamps[j] = t
		batch.Values[j] = v
		j++
		if j < size && !p.it.Next() {
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
