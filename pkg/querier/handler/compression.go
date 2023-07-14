package handler

import (
	"net/http"
	"strings"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/gzhttp"
	"github.com/klauspost/compress/zlib"
)

const (
	acceptEncodingHeader  = "Accept-Encoding"
	contentEncodingHeader = "Content-Encoding"
	snappyEncoding        = "snappy"
	gzipEncoding          = "gzip"
	deflateEncoding       = "deflate"
)

// Wrapper around http.ResponseWriter which adds deflate compression
type deflatedResponseWriter struct {
	http.ResponseWriter
	writer *zlib.Writer
}

func (c *deflatedResponseWriter) Write(p []byte) (int, error) {
	return c.writer.Write(p)
}

func (c *deflatedResponseWriter) Close() {
	c.writer.Close()
}

func newDeflateResponseWriter(writer http.ResponseWriter) *deflatedResponseWriter {
	return &deflatedResponseWriter{
		ResponseWriter: writer,
		writer:         zlib.NewWriter(writer),
	}
}

// Wrapper around http.ResponseWriter which adds deflate compression
type snappyResponseWriter struct {
	http.ResponseWriter
	writer *snappy.Writer
}

func (c *snappyResponseWriter) Write(p []byte) (int, error) {
	return c.writer.Write(p)
}

func (c *snappyResponseWriter) Close() {
	c.writer.Close()
}

func newSnappyResponseWriter(writer http.ResponseWriter) *snappyResponseWriter {
	return &snappyResponseWriter{
		ResponseWriter: writer,
		writer:         snappy.NewBufferedWriter(writer),
	}
}

// CompressionHandler is a wrapper around http.Handler which adds suitable
// response compression based on the client's Accept-Encoding headers.
type CompressionHandler struct {
	Handler http.Handler
}

// ServeHTTP adds compression to the original http.Handler's ServeHTTP() method.
func (c CompressionHandler) ServeHTTP(writer http.ResponseWriter, req *http.Request) {
	encodings := strings.Split(req.Header.Get(acceptEncodingHeader), ",")
	for _, encoding := range encodings {
		switch strings.TrimSpace(encoding) {
		case gzipEncoding:
			gzhttp.GzipHandler(c.Handler).ServeHTTP(writer, req)
			return
		case snappyEncoding:
			compWriter := newSnappyResponseWriter(writer)
			writer.Header().Set(contentEncodingHeader, snappyEncoding)
			c.Handler.ServeHTTP(compWriter, req)
			compWriter.Close()
			return
		case deflateEncoding:
			compWriter := newDeflateResponseWriter(writer)
			writer.Header().Set(contentEncodingHeader, deflateEncoding)
			c.Handler.ServeHTTP(compWriter, req)
			compWriter.Close()
			return
		default:
			c.Handler.ServeHTTP(writer, req)
			return
		}
	}
}
