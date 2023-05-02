package client

import (
	"encoding/binary"

	"github.com/cortexproject/cortex/pkg/chunk/encoding"
)

// ChunksCount returns the number of chunks in response.
func (m *QueryStreamResponse) ChunksCount() int {
	if len(m.Chunkseries) == 0 {
		return 0
	}

	count := 0
	for _, entry := range m.Chunkseries {
		count += len(entry.Chunks)
	}
	return count
}

// ChunksSize returns the size of all chunks in the response.
func (m *QueryStreamResponse) ChunksSize() int {
	if len(m.Chunkseries) == 0 {
		return 0
	}

	size := 0
	for _, entry := range m.Chunkseries {
		for _, chunk := range entry.Chunks {
			size += chunk.Size()
		}
	}
	return size
}

func (m *QueryStreamResponse) SamplesCount() (count int) {
	for _, ts := range m.Timeseries {
		count += len(ts.Samples)
	}
	for _, cs := range m.Chunkseries {
		for _, c := range cs.Chunks {
			if c.Encoding == int32(encoding.PrometheusXorChunk) {
				count += int(binary.BigEndian.Uint16(c.Data))
			}
		}
	}
	return
}
