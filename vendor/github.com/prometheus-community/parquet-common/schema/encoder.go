// Copyright The Prometheus Authors
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
	"io"
	"math"
	"slices"
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

	reEncodedChunks := make([]map[chunkenc.Encoding][]*chunks.Meta, dataColSize)
	reEncodedChunksAppenders := make([]map[chunkenc.Encoding]chunkenc.Appender, dataColSize)

	for i := 0; i < dataColSize; i++ {
		reEncodedChunks[i] = make(map[chunkenc.Encoding][]*chunks.Meta)
		reEncodedChunksAppenders[i] = make(map[chunkenc.Encoding]chunkenc.Appender)

		for _, enc := range []chunkenc.Encoding{chunkenc.EncXOR, chunkenc.EncHistogram, chunkenc.EncFloatHistogram} {
			var chunk chunkenc.Chunk
			switch enc {
			case chunkenc.EncXOR:
				chunk = chunkenc.NewXORChunk()
			case chunkenc.EncHistogram:
				chunk = chunkenc.NewHistogramChunk()
			case chunkenc.EncFloatHistogram:
				chunk = chunkenc.NewFloatHistogramChunk()
			default:
				return nil, fmt.Errorf("unknown encoding %v", enc)
			}

			reEncodedChunks[i][enc] = append(reEncodedChunks[i][enc], &chunks.Meta{
				Chunk:   chunk,
				MinTime: math.MaxInt64,
			})
			app, err := reEncodedChunks[i][enc][0].Chunk.Appender()
			if err != nil {
				return nil, err
			}
			reEncodedChunksAppenders[i][enc] = app
		}
	}

	var sampleIt chunkenc.Iterator
	for _, chk := range chks {
		sampleIt = chk.Chunk.Iterator(sampleIt)
		switch chk.Chunk.Encoding() {
		case chunkenc.EncXOR:
			for vt := sampleIt.Next(); vt != chunkenc.ValNone; vt = sampleIt.Next() {
				if vt != chunkenc.ValFloat {
					return nil, fmt.Errorf("found value type %v in float chunk", vt)
				}
				t, v := sampleIt.At()

				chkIdx := e.schema.DataColumIdx(t)
				reEncodedChunksAppenders[chkIdx][chunkenc.EncXOR].Append(t, v)
				if t < reEncodedChunks[chkIdx][chunkenc.EncXOR][len(reEncodedChunks[chkIdx][chunkenc.EncXOR])-1].MinTime {
					reEncodedChunks[chkIdx][chunkenc.EncXOR][len(reEncodedChunks[chkIdx][chunkenc.EncXOR])-1].MinTime = t
				}
				if t > reEncodedChunks[chkIdx][chunkenc.EncXOR][len(reEncodedChunks[chkIdx][chunkenc.EncXOR])-1].MaxTime {
					reEncodedChunks[chkIdx][chunkenc.EncXOR][len(reEncodedChunks[chkIdx][chunkenc.EncXOR])-1].MaxTime = t
				}
			}
		case chunkenc.EncFloatHistogram:
			for vt := sampleIt.Next(); vt != chunkenc.ValNone; vt = sampleIt.Next() {
				if vt != chunkenc.ValFloatHistogram {
					return nil, fmt.Errorf("found value type %v in float histogram chunk", vt)
				}
				t, v := sampleIt.AtFloatHistogram(nil)

				chkIdx := e.schema.DataColumIdx(t)
				newC, recoded, app, err := reEncodedChunksAppenders[chkIdx][chunkenc.EncFloatHistogram].AppendFloatHistogram(nil, t, v, false)
				if err != nil {
					return nil, err
				}
				reEncodedChunksAppenders[chkIdx][chunkenc.EncFloatHistogram] = app
				if newC != nil {
					if !recoded {
						reEncodedChunks[chkIdx][chunkenc.EncFloatHistogram] = append(reEncodedChunks[chkIdx][chunkenc.EncFloatHistogram], &chunks.Meta{
							MinTime: math.MaxInt64,
						})
					}
					reEncodedChunks[chkIdx][chunkenc.EncFloatHistogram][len(reEncodedChunks[chkIdx][chunkenc.EncFloatHistogram])-1].Chunk = newC
				}

				if t < reEncodedChunks[chkIdx][chunkenc.EncFloatHistogram][len(reEncodedChunks[chkIdx][chunkenc.EncFloatHistogram])-1].MinTime {
					reEncodedChunks[chkIdx][chunkenc.EncFloatHistogram][len(reEncodedChunks[chkIdx][chunkenc.EncFloatHistogram])-1].MinTime = t
				}
				if t > reEncodedChunks[chkIdx][chunkenc.EncFloatHistogram][len(reEncodedChunks[chkIdx][chunkenc.EncFloatHistogram])-1].MaxTime {
					reEncodedChunks[chkIdx][chunkenc.EncFloatHistogram][len(reEncodedChunks[chkIdx][chunkenc.EncFloatHistogram])-1].MaxTime = t
				}
			}
		case chunkenc.EncHistogram:
			for vt := sampleIt.Next(); vt != chunkenc.ValNone; vt = sampleIt.Next() {
				if vt != chunkenc.ValHistogram {
					return nil, fmt.Errorf("found value type %v in histogram chunk", vt)
				}
				t, v := sampleIt.AtHistogram(nil)

				chkIdx := e.schema.DataColumIdx(t)
				newC, recoded, app, err := reEncodedChunksAppenders[chkIdx][chunkenc.EncHistogram].AppendHistogram(nil, t, v, false)
				if err != nil {
					return nil, err
				}
				reEncodedChunksAppenders[chkIdx][chunkenc.EncHistogram] = app
				if newC != nil {
					if !recoded {
						reEncodedChunks[chkIdx][chunkenc.EncHistogram] = append(reEncodedChunks[chkIdx][chunkenc.EncHistogram], &chunks.Meta{
							MinTime: math.MaxInt64,
						})
					}
					reEncodedChunks[chkIdx][chunkenc.EncHistogram][len(reEncodedChunks[chkIdx][chunkenc.EncHistogram])-1].Chunk = newC
				}

				if t < reEncodedChunks[chkIdx][chunkenc.EncHistogram][len(reEncodedChunks[chkIdx][chunkenc.EncHistogram])-1].MinTime {
					reEncodedChunks[chkIdx][chunkenc.EncHistogram][len(reEncodedChunks[chkIdx][chunkenc.EncHistogram])-1].MinTime = t
				}
				if t > reEncodedChunks[chkIdx][chunkenc.EncHistogram][len(reEncodedChunks[chkIdx][chunkenc.EncHistogram])-1].MaxTime {
					reEncodedChunks[chkIdx][chunkenc.EncHistogram][len(reEncodedChunks[chkIdx][chunkenc.EncHistogram])-1].MaxTime = t
				}
			}
		default:
			return nil, fmt.Errorf("unknown encoding %v", chk.Chunk.Encoding())
		}
	}

	result := make([][]byte, dataColSize)

	for i, chunks := range reEncodedChunks {
		for _, enc := range []chunkenc.Encoding{chunkenc.EncXOR, chunkenc.EncHistogram, chunkenc.EncFloatHistogram} {
			for _, chk := range chunks[enc] {
				if chk.Chunk.NumSamples() == 0 {
					continue
				}
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
		}
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

	for {
		chkEnc, err := binary.ReadUvarint(b)
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		minTime, err := binary.ReadUvarint(b)
		if err != nil {
			return nil, err
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
		b.Next(int(size))

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

func EncodeIntSlice(s []int) []byte {
	l := make([]byte, binary.MaxVarintLen32)
	r := make([]byte, 0, len(s)*binary.MaxVarintLen32)

	// Sort to compress more efficiently
	slices.Sort(s)

	// size
	n := binary.PutVarint(l[:], int64(len(s)))
	r = append(r, l[:n]...)

	for i := 0; i < len(s); i++ {
		n := binary.PutVarint(l[:], int64(s[i]))
		r = append(r, l[:n]...)
	}

	return r
}

func DecodeUintSlice(b []byte) ([]int, error) {
	buffer := bytes.NewBuffer(b)

	// size
	s, err := binary.ReadVarint(buffer)
	if err != nil {
		return nil, err
	}

	r := make([]int, 0, s)

	for i := int64(0); i < s; i++ {
		v, err := binary.ReadVarint(buffer)
		if err != nil {
			return nil, err
		}
		r = append(r, int(v))
	}

	return r, nil
}
