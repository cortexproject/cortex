package chunk

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"

	"github.com/prometheus/common/model"
	"github.com/prometheus/tsdb/chunkenc"
)

const samplesPerChunk = 120

var errOutOfBounds = errors.New("out of bounds")

// bigchunk is a set of prometheus/tsdb chunks.  It grows over time and has no
// upperbound on number of samples it can contain.
type bigchunk struct {
	chunks           []chunkenc.Chunk
	appender         chunkenc.Appender
	remainingSamples int
}

func newBigchunk() *bigchunk {
	return &bigchunk{}
}

func (b *bigchunk) Add(sample model.SamplePair) ([]Chunk, error) {
	if b.remainingSamples == 0 {
		chunk := chunkenc.NewXORChunk()
		appender, err := chunk.Appender()
		if err != nil {
			return nil, err
		}

		b.chunks = append(b.chunks, chunk)
		b.appender = appender
		b.remainingSamples = samplesPerChunk
	}

	b.appender.Append(int64(sample.Timestamp), float64(sample.Value))
	b.remainingSamples--
	return []Chunk{b}, nil
}

func (b *bigchunk) Marshal(wio io.Writer) error {
	w := writer{wio}
	if err := w.WriteVarInt16(uint16(len(b.chunks))); err != nil {
		return err
	}
	for _, chunk := range b.chunks {
		buf := chunk.Bytes()
		if err := w.WriteVarInt16(uint16(len(buf))); err != nil {
			return err
		}
		if _, err := w.Write(buf); err != nil {
			return err
		}
	}
	return nil
}

func (b *bigchunk) MarshalToBuf(buf []byte) error {
	writer := bytes.NewBuffer(buf)
	return b.Marshal(writer)
}

func (b *bigchunk) Unmarshal(r io.Reader) error {
	buf, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	return b.UnmarshalFromBuf(buf)
}

func (b *bigchunk) UnmarshalFromBuf(buf []byte) error {
	r := reader{buf: buf}
	numChunks, err := r.ReadUint16()
	if err != nil {
		return err
	}

	b.chunks = make([]chunkenc.Chunk, 0, numChunks)
	for i := uint16(0); i < numChunks; i++ {
		chunkLen, err := r.ReadUint16()
		if err != nil {
			return err
		}

		chunkBuf, err := r.ReadBytes(int(chunkLen))
		if err != nil {
			return err
		}

		chunk, err := chunkenc.FromData(chunkenc.EncXOR, chunkBuf)
		if err != nil {
			return err
		}

		b.chunks = append(b.chunks, chunk)
	}
	return nil
}

func (b *bigchunk) Encoding() Encoding {
	return Bigchunk
}

func (b *bigchunk) Utilization() float64 {
	return 1.0
}

func (b *bigchunk) Len() int {
	sum := 0
	for _, c := range b.chunks {
		sum += c.NumSamples()
	}
	return sum
}

func (b *bigchunk) NewIterator() Iterator {
	return &bigchunkIterator{
		bigchunk: b,
	}
}

type writer struct {
	io.Writer
}

func (w writer) WriteVarInt16(i uint16) error {
	var b [2]byte
	binary.LittleEndian.PutUint16(b[:], i)
	_, err := w.Write(b[:])
	return err
}

type reader struct {
	i   int
	buf []byte
}

func (r *reader) ReadUint16() (uint16, error) {
	if r.i+2 > len(r.buf) {
		return 0, errOutOfBounds
	}
	result := binary.LittleEndian.Uint16(r.buf[r.i:])
	r.i += 2
	return result, nil
}

func (r *reader) ReadBytes(count int) ([]byte, error) {
	if r.i+count > len(r.buf) {
		return nil, errOutOfBounds
	}
	result := r.buf[r.i : r.i+count]
	r.i += count
	return result, nil
}

type bigchunkIterator struct {
	*bigchunk

	iter chunkenc.Iterator
	i    int
}

func (i *bigchunkIterator) FindAtOrAfter(target model.Time) bool {
	i.i = 0
	for i.i < len(i.chunks) {
		i.iter = i.chunks[i.i].Iterator()
		i.i++

		for i.iter.Next() {
			t, _ := i.iter.At()
			if t >= int64(target) {
				return true
			}
		}
	}
	return false
}

func (i *bigchunkIterator) Scan() bool {
	if i.iter != nil && i.iter.Next() {
		return true
	}

	for i.i < len(i.chunks) {
		i.iter = i.chunks[i.i].Iterator()
		i.i++
		if i.iter.Next() {
			return true
		}
	}
	return false
}

func (i *bigchunkIterator) Value() model.SamplePair {
	t, v := i.iter.At()
	return model.SamplePair{
		Timestamp: model.Time(t),
		Value:     model.SampleValue(v),
	}
}

func (i *bigchunkIterator) Batch(size int) Batch {
	var result Batch
	j := 0
	for j < size {
		t, v := i.iter.At()
		result.Timestamps[j] = t
		result.Values[j] = v
		j++
		if j < size && !i.Scan() {
			break
		}
	}
	result.Length = j
	return result
}

func (i *bigchunkIterator) Err() error {
	if i.iter != nil {
		return i.iter.Err()
	}
	return nil
}
