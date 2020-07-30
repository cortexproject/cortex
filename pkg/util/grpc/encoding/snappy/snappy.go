package snappy

import (
	"io"
	"sync"

	"github.com/golang/snappy"
	"google.golang.org/grpc/encoding"
)

// Name is the name registered for the snappy compressor.
const Name = "snappy"

func init() {
	encoding.RegisterCompressor(newCompressor())
}

type compressor struct {
	writersPool sync.Pool
	readersPool sync.Pool
}

func newCompressor() *compressor {
	c := &compressor{}
	c.readersPool = sync.Pool{
		New: func() interface{} {
			return &reader{
				pool:   &c.readersPool,
				Reader: snappy.NewReader(nil),
			}
		},
	}
	c.writersPool = sync.Pool{
		New: func() interface{} {
			return &writeCloser{
				pool:   &c.writersPool,
				Writer: snappy.NewBufferedWriter(nil),
			}
		},
	}
	return c
}

func (c *compressor) Name() string {
	return Name
}

func (c *compressor) Compress(w io.Writer) (io.WriteCloser, error) {
	wr := c.writersPool.Get().(*writeCloser)
	wr.Reset(w)
	return wr, nil
}

func (c *compressor) Decompress(r io.Reader) (io.Reader, error) {
	dr := c.readersPool.Get().(*reader)
	dr.Reset(r)
	return dr, nil
}

type writeCloser struct {
	*snappy.Writer
	pool *sync.Pool
}

func (w *writeCloser) Close() error {
	defer w.pool.Put(w)
	return w.Writer.Close()
}

type reader struct {
	*snappy.Reader
	pool *sync.Pool
}

func (r *reader) Read(p []byte) (n int, err error) {
	n, err = r.Reader.Read(p)
	if err == io.EOF {
		r.pool.Put(r)
	}
	return n, err
}
