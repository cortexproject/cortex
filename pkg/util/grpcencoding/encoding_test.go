package grpcencoding

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/cortexproject/cortex/pkg/util/grpcencoding/snappy"
	"github.com/cortexproject/cortex/pkg/util/grpcencoding/snappyblock"
	"github.com/cortexproject/cortex/pkg/util/grpcencoding/zstd"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/encoding"
)

func TestCompressors(t *testing.T) {
	testCases := []struct {
		name string
	}{
		{
			name: snappy.Name,
		},
		{
			name: snappyblock.Name,
		},
		{
			name: zstd.Name,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testCompress(tc.name, t)
		})
	}
}

func testCompress(name string, t *testing.T) {
	c := encoding.GetCompressor(name)
	assert.Equal(t, name, c.Name())

	tests := []struct {
		test  string
		input string
	}{
		{"empty", ""},
		{"short", "hello world"},
		{"long", strings.Repeat("123456789", 2024)},
	}
	for _, test := range tests {
		t.Run(test.test, func(t *testing.T) {
			buf := &bytes.Buffer{}
			// Compress
			w, err := c.Compress(buf)
			require.NoError(t, err)
			n, err := w.Write([]byte(test.input))
			require.NoError(t, err)
			assert.Len(t, test.input, n)
			err = w.Close()
			require.NoError(t, err)
			compressedBytes := buf.Bytes()
			buf = bytes.NewBuffer(compressedBytes)

			// Decompress
			r, err := c.Decompress(buf)
			require.NoError(t, err)
			out, err := io.ReadAll(r)
			require.NoError(t, err)
			assert.Equal(t, test.input, string(out))

			if sizer, ok := c.(interface {
				DecompressedSize(compressedBytes []byte) int
			}); ok {
				buf = bytes.NewBuffer(compressedBytes)
				r, err := c.Decompress(buf)
				require.NoError(t, err)
				out, err := io.ReadAll(r)
				require.NoError(t, err)
				assert.Equal(t, len(out), sizer.DecompressedSize(compressedBytes))
			}
		})
	}
}

func BenchmarkCompress(b *testing.B) {
	data := []byte(strings.Repeat("123456789", 1024))

	testCases := []struct {
		name string
	}{
		{
			name: snappy.Name,
		},
		{
			name: snappyblock.Name,
		},
		{
			name: zstd.Name,
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			c := encoding.GetCompressor(tc.name)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				w, _ := c.Compress(io.Discard)
				_, _ = w.Write(data)
				_ = w.Close()
			}
			b.ReportAllocs()
		})
	}
}

func BenchmarkDecompress(b *testing.B) {
	data := []byte(strings.Repeat("123456789", 1024))

	testCases := []struct {
		name string
	}{
		{
			name: snappy.Name,
		},
		{
			name: snappyblock.Name,
		},
		{
			name: zstd.Name,
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			c := encoding.GetCompressor(tc.name)
			var buf bytes.Buffer
			w, _ := c.Compress(&buf)
			_, _ = w.Write(data)
			w.Close()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, err := decompress(c, buf.Bytes(), 10000)
				require.NoError(b, err)
			}
			b.ReportAllocs()
		})
	}
}

// This function was copied from: https://github.com/grpc/grpc-go/blob/70c52915099a3b30848d0cb22e2f8951dd5aed7f/rpc_util.go#L765
func decompress(compressor encoding.Compressor, d []byte, maxReceiveMessageSize int) ([]byte, int, error) {
	dcReader, err := compressor.Decompress(bytes.NewReader(d))
	if err != nil {
		return nil, 0, err
	}
	if sizer, ok := compressor.(interface {
		DecompressedSize(compressedBytes []byte) int
	}); ok {
		if size := sizer.DecompressedSize(d); size >= 0 {
			if size > maxReceiveMessageSize {
				return nil, size, nil
			}
			// size is used as an estimate to size the buffer, but we
			// will read more data if available.
			// +MinRead so ReadFrom will not reallocate if size is correct.
			buf := bytes.NewBuffer(make([]byte, 0, size+bytes.MinRead))
			bytesRead, err := buf.ReadFrom(io.LimitReader(dcReader, int64(maxReceiveMessageSize)+1))
			return buf.Bytes(), int(bytesRead), err
		}
	}
	// Read from LimitReader with limit max+1. So if the underlying
	// reader is over limit, the result will be bigger than max.
	d, err = io.ReadAll(io.LimitReader(dcReader, int64(maxReceiveMessageSize)+1))
	return d, len(d), err
}
