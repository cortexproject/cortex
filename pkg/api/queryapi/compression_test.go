package queryapi

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zlib"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func decompress(t *testing.T, encoding string, b []byte) []byte {
	t.Helper()

	switch encoding {
	case gzipEncoding:
		r, err := gzip.NewReader(bytes.NewReader(b))
		require.NoError(t, err)
		defer r.Close()
		data, err := io.ReadAll(r)
		require.NoError(t, err)
		return data
	case deflateEncoding:
		r, err := zlib.NewReader(bytes.NewReader(b))
		require.NoError(t, err)
		defer r.Close()
		data, err := io.ReadAll(r)
		require.NoError(t, err)
		return data
	case snappyEncoding:
		data, err := io.ReadAll(snappy.NewReader(bytes.NewReader(b)))
		require.NoError(t, err)
		return data
	case zstdEncoding:
		r, err := zstd.NewReader(bytes.NewReader(b))
		require.NoError(t, err)
		defer r.Close()
		data, err := io.ReadAll(r)
		require.NoError(t, err)
		return data
	default:
		return b
	}
}

func TestNewCompressedResponseWriter_SupportedEncodings(t *testing.T) {
	for _, tc := range []string{gzipEncoding, deflateEncoding, snappyEncoding, zstdEncoding} {
		t.Run(tc, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set(acceptEncodingHeader, tc)

			cw := newCompressedResponseWriter(rec, req)
			payload := []byte("hello world")
			_, err := cw.Write(payload)
			require.NoError(t, err)
			cw.Close()

			require.Equal(t, tc, rec.Header().Get(contentEncodingHeader))

			decompressed := decompress(t, tc, rec.Body.Bytes())
			require.Equal(t, payload, decompressed)

			switch tc {
			case gzipEncoding:
				_, ok := cw.writer.(*gzip.Writer)
				require.True(t, ok)
			case deflateEncoding:
				_, ok := cw.writer.(*zlib.Writer)
				require.True(t, ok)
			case snappyEncoding:
				_, ok := cw.writer.(*snappy.Writer)
				require.True(t, ok)
			case zstdEncoding:
				_, ok := cw.writer.(*zstd.Encoder)
				require.True(t, ok)
			}
		})
	}
}

func TestNewCompressedResponseWriter_UnsupportedEncoding(t *testing.T) {
	for _, tc := range []string{"", "br", "unknown"} {
		t.Run(tc, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tc != "" {
				req.Header.Set(acceptEncodingHeader, tc)
			}

			cw := newCompressedResponseWriter(rec, req)
			payload := []byte("data")
			_, err := cw.Write(payload)
			require.NoError(t, err)
			cw.Close()

			require.Empty(t, rec.Header().Get(contentEncodingHeader))
			require.Equal(t, payload, rec.Body.Bytes())
			require.Same(t, rec, cw.writer)
		})
	}
}

func TestNewCompressedResponseWriter_MultipleEncodings(t *testing.T) {
	tests := []struct {
		header     string
		expectEnc  string
		expectType interface{}
	}{
		{"snappy, gzip", snappyEncoding, &snappy.Writer{}},
		{"unknown, gzip", gzipEncoding, &gzip.Writer{}},
	}

	for _, tc := range tests {
		t.Run(tc.header, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set(acceptEncodingHeader, tc.header)

			cw := newCompressedResponseWriter(rec, req)
			_, err := cw.Write([]byte("payload"))
			require.NoError(t, err)
			cw.Close()

			require.Equal(t, tc.expectEnc, rec.Header().Get(contentEncodingHeader))
			decompressed := decompress(t, tc.expectEnc, rec.Body.Bytes())
			require.Equal(t, []byte("payload"), decompressed)

			switch tc.expectEnc {
			case gzipEncoding:
				require.IsType(t, &gzip.Writer{}, cw.writer)
			case snappyEncoding:
				require.IsType(t, &snappy.Writer{}, cw.writer)
			}
		})
	}
}

func TestCompressionHandler_ServeHTTP(t *testing.T) {
	handler := CompressionHandler{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, err := w.Write([]byte("hello"))
		require.NoError(t, err)
	})}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(acceptEncodingHeader, gzipEncoding)

	handler.ServeHTTP(rec, req)

	require.Equal(t, gzipEncoding, rec.Header().Get(contentEncodingHeader))
	decompressed := decompress(t, gzipEncoding, rec.Body.Bytes())
	require.Equal(t, []byte("hello"), decompressed)
}
