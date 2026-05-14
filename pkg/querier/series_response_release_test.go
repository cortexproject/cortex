package querier

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"google.golang.org/grpc/mem"

	"github.com/cortexproject/cortex/pkg/cortexpb"
)

func TestSeriesResponseImplementsReleasableMessage(t *testing.T) {
	var resp storepb.SeriesResponse
	var _ cortexpb.ReleasableMessage = &resp

	assert.NotPanics(t, func() { resp.Free() })
}

func TestSeriesResponseFreeIdempotence(t *testing.T) {
	t.Run("zero value", func(t *testing.T) {
		var resp storepb.SeriesResponse
		assert.NotPanics(t, func() { resp.Free() })
		assert.NotPanics(t, func() { resp.Free() })
	})

	t.Run("with registered buffer", func(t *testing.T) {
		var resp storepb.SeriesResponse
		buf := mem.SliceBuffer(make([]byte, 64))
		resp.RegisterBuffer(buf)

		assert.NotPanics(t, func() { resp.Free() })
		assert.NotPanics(t, func() { resp.Free() })
	})

	t.Run("with pooled buffer", func(t *testing.T) {
		var resp storepb.SeriesResponse
		b := make([]byte, 128)
		resp.RegisterBuffer(mem.NewBuffer(&b, mem.NopBufferPool{}))

		assert.NotPanics(t, func() { resp.Free() })
		assert.NotPanics(t, func() { resp.Free() })
	})
}

func TestDetachSeriesFromBuffer_NoUseAfterFree(t *testing.T) {
	t.Run("labels survive buffer overwrite", func(t *testing.T) {
		// ZLabel strings are unsafe casts into the unmarshal buffer.
		bufData := []byte("__name__\x00http_requests_total\x00cluster\x00us-east-1\x00")
		series := &storepb.Series{
			Labels: []storepb.Label{
				{Name: unsafeString(bufData[0:8]), Value: unsafeString(bufData[9:28])},
				{Name: unsafeString(bufData[29:36]), Value: unsafeString(bufData[37:46])},
			},
		}

		detachSeriesFromBuffer(series)

		// Overwrite original buffer (simulates pool reuse).
		for i := range bufData {
			bufData[i] = 0xFF
		}

		assert.Equal(t, "__name__", series.Labels[0].Name)
		assert.Equal(t, "http_requests_total", series.Labels[0].Value)
		assert.Equal(t, "cluster", series.Labels[1].Name)
		assert.Equal(t, "us-east-1", series.Labels[1].Value)
	})

	t.Run("chunk data survives buffer overwrite", func(t *testing.T) {
		chunkBuf := make([]byte, 4096)
		for i := range chunkBuf {
			chunkBuf[i] = byte(i % 256)
		}
		expected := make([]byte, len(chunkBuf))
		copy(expected, chunkBuf)

		series := &storepb.Series{
			Chunks: []storepb.AggrChunk{
				{MinTime: 1000, MaxTime: 2000, Raw: &storepb.Chunk{Type: storepb.Chunk_XOR, Data: chunkBuf}},
			},
		}

		detachSeriesFromBuffer(series)

		for i := range chunkBuf {
			chunkBuf[i] = 0xFF
		}

		assert.Equal(t, expected, series.Chunks[0].Raw.Data)
	})

	t.Run("end-to-end with Free and buffer overwrite", func(t *testing.T) {
		chunkData := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
		series := &storepb.Series{
			Labels: []storepb.Label{{Name: "job", Value: "prometheus"}},
			Chunks: []storepb.AggrChunk{
				{Raw: &storepb.Chunk{Type: storepb.Chunk_XOR, Data: chunkData}},
			},
		}
		resp := &storepb.SeriesResponse{
			Result: &storepb.SeriesResponse_Series{Series: series},
		}

		poolBuf := make([]byte, 32768)
		resp.RegisterBuffer(mem.NewBuffer(&poolBuf, mem.NopBufferPool{}))

		s := resp.GetSeries()
		detachSeriesFromBuffer(s)
		resp.Free()

		// Overwrite both the pool buffer and original chunk slice.
		for i := range poolBuf {
			poolBuf[i] = 0xDE
		}
		for i := range chunkData {
			chunkData[i] = 0xAB
		}

		assert.Equal(t, "job", s.Labels[0].Name)
		assert.Equal(t, "prometheus", s.Labels[0].Value)
		assert.Equal(t, []byte{0x01, 0x02, 0x03, 0x04, 0x05}, s.Chunks[0].Raw.Data)
	})
}

// unsafeString creates a string sharing memory with the byte slice,
// simulating protobuf's zero-copy string unmarshal.
func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
