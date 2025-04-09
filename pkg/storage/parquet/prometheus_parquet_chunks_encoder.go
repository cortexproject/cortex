package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/thanos-io/thanos/pkg/compact/downsample"

	"github.com/dennwc/varint"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

type PrometheusParquetChunksEncoder struct {
}

type PrometheusParquetChunksDecoder struct {
	Pool chunkenc.Pool
}

func NewPrometheusParquetChunksEncoder() *PrometheusParquetChunksEncoder {
	return &PrometheusParquetChunksEncoder{}
}

func NewPrometheusParquetChunksDecoder() *PrometheusParquetChunksDecoder {
	return &PrometheusParquetChunksDecoder{
		Pool: downsample.NewPool(),
	}
}

func (e *PrometheusParquetChunksEncoder) Close() error {
	return nil
}

func (e *PrometheusParquetChunksEncoder) Encode(chks []chunks.Meta) ([][]byte, error) {
	sort.Slice(chks, func(i, j int) bool {
		return chks[i].MinTime < chks[j].MinTime
	})

	reEncodedChunks := make([]chunks.Meta, ChunkColumnsPerDay)
	reEncodedChunksAppenders := make([]chunkenc.Appender, ChunkColumnsPerDay)

	for i := 0; i < ChunkColumnsPerDay; i++ {
		reEncodedChunks[i] = chunks.Meta{
			Chunk:   chunkenc.NewXORChunk(),
			MinTime: math.MaxInt64,
		}
		app, err := reEncodedChunks[i].Chunk.Appender()
		if err != nil {
			return nil, err
		}
		reEncodedChunksAppenders[i] = app
	}

	var it chunkenc.Iterator
	for _, chk := range chks {
		it = chk.Chunk.Iterator(it)
		switch chk.Chunk.Encoding() {
		case chunkenc.EncXOR:
			for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
				if vt != chunkenc.ValFloat {
					return nil, fmt.Errorf("found value type %v in float chunk", vt)
				}
				t, v := it.At()
				hour := time.UnixMilli(t).UTC().Hour()
				chkIdx := (hour / int(ChunkColumnLength.Hours())) % ChunkColumnsPerDay
				reEncodedChunksAppenders[chkIdx].Append(t, v)
				if t < reEncodedChunks[chkIdx].MinTime {
					reEncodedChunks[chkIdx].MinTime = t
				}
				if t > reEncodedChunks[chkIdx].MaxTime {
					reEncodedChunks[chkIdx].MaxTime = t
				}
			}
		default:
			continue
		}
	}

	result := make([][]byte, ChunkColumnsPerDay)

	for i, chk := range reEncodedChunks {
		var b [varint.MaxLen64]byte
		n := binary.PutUvarint(b[:], uint64(chk.MinTime))
		result[i] = append(result[i], b[:n]...)
		n = binary.PutUvarint(b[:], uint64(chk.MaxTime))
		result[i] = append(result[i], b[:n]...)
		n = binary.PutUvarint(b[:], uint64(chk.Chunk.Encoding()))
		result[i] = append(result[i], b[:n]...)
		n = binary.PutUvarint(b[:], uint64(len(chk.Chunk.Bytes())))
		result[i] = append(result[i], b[:n]...)
		result[i] = append(result[i], chk.Chunk.Bytes()...)
	}
	return result, nil
}

func (e *PrometheusParquetChunksDecoder) Decode(data [][]byte, mint, maxt int64) ([]chunks.Meta, error) {
	result := make([]chunks.Meta, 0, len(data))

	for _, dChunk := range data {
		b := bytes.NewBuffer(dChunk)
		c := chunks.Meta{}
		minTime, err := binary.ReadUvarint(b)
		if err != nil {
			return nil, err
		}
		c.MinTime = int64(minTime)
		maxTime, err := binary.ReadUvarint(b)
		if err != nil {
			return nil, err
		}
		c.MinTime = int64(maxTime)
		chkEnc, err := binary.ReadUvarint(b)
		if err != nil {
			return nil, err
		}
		size, err := binary.ReadUvarint(b)
		if err != nil {
			return nil, err
		}
		cData := b.Bytes()[:size]
		chk, err := e.Pool.Get(chunkenc.Encoding(chkEnc), cData)
		c.Chunk = chk
		if int64(minTime) > maxt {
			continue
		}

		if int64(maxTime) >= mint {
			result = append(result, chunks.Meta{
				MinTime: int64(minTime),
				MaxTime: int64(maxTime),
				Chunk:   chk,
			})
		}
	}

	return result, nil
}
