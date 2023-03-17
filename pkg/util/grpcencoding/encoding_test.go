package grpcencoding

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/cortexproject/cortex/pkg/util/grpcencoding/snappy"
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
		{"long", strings.Repeat("123456789", 1024)},
	}
	for _, test := range tests {
		t.Run(test.test, func(t *testing.T) {
			var buf bytes.Buffer
			// Compress
			w, err := c.Compress(&buf)
			require.NoError(t, err)
			n, err := w.Write([]byte(test.input))
			require.NoError(t, err)
			assert.Len(t, test.input, n)
			err = w.Close()
			require.NoError(t, err)
			// Decompress
			r, err := c.Decompress(&buf)
			require.NoError(t, err)
			out, err := io.ReadAll(r)
			require.NoError(t, err)
			assert.Equal(t, test.input, string(out))
		})
	}
}

func BenchmarkSnappyCompress(b *testing.B) {
	data := []byte(strings.Repeat("123456789", 1024))

	testCases := []struct {
		name string
	}{
		{
			name: snappy.Name,
		},
		{
			name: zstd.Name,
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			c := encoding.GetCompressor("snappy")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				w, _ := c.Compress(io.Discard)
				_, _ = w.Write(data)
				_ = w.Close()
			}
		})
	}
}

func BenchmarkSnappyDecompress(b *testing.B) {
	data := []byte(strings.Repeat("123456789", 1024))

	testCases := []struct {
		name string
	}{
		{
			name: snappy.Name,
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
			reader := bytes.NewReader(buf.Bytes())
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r, _ := c.Decompress(reader)
				_, _ = io.ReadAll(r)
				_, _ = reader.Seek(0, io.SeekStart)
			}
		})
	}
}
