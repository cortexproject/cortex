// Copyright 2021 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schema

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"github.com/dennwc/varint"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

type PrometheusParquetChunksEncoder struct {
	schema *TSDBSchema
}

func NewPrometheusParquetChunksEncoder(schema *TSDBSchema) *PrometheusParquetChunksEncoder {
	return &PrometheusParquetChunksEncoder{
		schema: schema,
	}
}

func (e *PrometheusParquetChunksEncoder) Encode(it chunks.Iterator) ([][]byte, error) {
	// NOTE: usually 'it' should hold chunks for one day. Chunks are usually length 2h so we should get 12 of them.
	chks := make([]chunks.Meta, 0, 12)
	for it.Next() {
		chks = append(chks, it.At())
	}

	sort.Slice(chks, func(i, j int) bool {
		return chks[i].MinTime < chks[j].MinTime
	})

	dataColSize := len(e.schema.DataColsIndexes)

	reEncodedChunks := make([]chunks.Meta, dataColSize)
	reEncodedChunksAppenders := make([]chunkenc.Appender, dataColSize)

	for i := 0; i < dataColSize; i++ {
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

	var sampleIt chunkenc.Iterator
	for _, chk := range chks {
		sampleIt = chk.Chunk.Iterator(sampleIt)
		switch chk.Chunk.Encoding() {
		case chunkenc.EncXOR:
			for vt := sampleIt.Next(); vt != chunkenc.ValNone; vt = sampleIt.Next() {
				// TODO: Native histograms support
				if vt != chunkenc.ValFloat {
					return nil, fmt.Errorf("found value type %v in float chunk", vt)
				}
				t, v := sampleIt.At()
				chkIdx := e.schema.DataColumIdx(t)
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

	result := make([][]byte, dataColSize)

	for i, chk := range reEncodedChunks {
		var b [varint.MaxLen64]byte
		n := binary.PutUvarint(b[:], uint64(chk.Chunk.Encoding()))
		result[i] = append(result[i], b[:n]...)
		n = binary.PutUvarint(b[:], uint64(chk.MinTime))
		result[i] = append(result[i], b[:n]...)
		n = binary.PutUvarint(b[:], uint64(chk.MaxTime))
		result[i] = append(result[i], b[:n]...)
		n = binary.PutUvarint(b[:], uint64(len(chk.Chunk.Bytes())))
		result[i] = append(result[i], b[:n]...)
		result[i] = append(result[i], chk.Chunk.Bytes()...)
	}
	return result, nil
}

type PrometheusParquetChunksDecoder struct {
	Pool chunkenc.Pool
}

func NewPrometheusParquetChunksDecoder(pool chunkenc.Pool) *PrometheusParquetChunksDecoder {
	return &PrometheusParquetChunksDecoder{
		Pool: pool,
	}
}

func (e *PrometheusParquetChunksDecoder) Decode(data []byte, mint, maxt int64) ([]chunks.Meta, error) {
	result := make([]chunks.Meta, 0, len(data))

	b := bytes.NewBuffer(data)

	chkEnc, err := binary.ReadUvarint(b)
	if err != nil {
		return nil, err
	}

	minTime, err := binary.ReadUvarint(b)
	if err != nil {
		return nil, err
	}
	if int64(minTime) > maxt {
		return nil, nil
	}

	maxTime, err := binary.ReadUvarint(b)
	if err != nil {
		return nil, err
	}
	size, err := binary.ReadUvarint(b)
	if err != nil {
		return nil, err
	}
	cData := b.Bytes()[:size]
	chk, err := e.Pool.Get(chunkenc.Encoding(chkEnc), cData)
	if err != nil {
		return nil, err
	}

	if int64(maxTime) >= mint {
		result = append(result, chunks.Meta{
			MinTime: int64(minTime),
			MaxTime: int64(maxTime),
			Chunk:   chk,
		})
	}

	return result, nil
}
