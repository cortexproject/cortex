package snappyblock

import (
	"bytes"
	"io"
	"sync"

	"github.com/golang/snappy"
	"google.golang.org/grpc/encoding"
)

// Name is the name registered for the snappy compressor.
const Name = "snappy-block"

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
				pool:  &c.readersPool,
				cbuff: bytes.NewBuffer(make([]byte, 0, 512)),
			}
		},
	}
	c.writersPool = sync.Pool{
		New: func() interface{} {
			return &writeCloser{
				pool: &c.writersPool,
				buff: bytes.NewBuffer(make([]byte, 0, 512)),
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
	err := dr.Reset(r)
	if err != nil {
		return nil, err
	}

	return dr, nil
}

// DecompressedSize If a Compressor implements DecompressedSize(compressedBytes []byte) int,
// gRPC will call it to determine the size of the buffer allocated for the
// result of decompression.
// Return -1 to indicate unknown size.
//
// This is an EXPERIMENTAL feature of grpc-go.
func (c *compressor) DecompressedSize(compressedBytes []byte) int {
	decompressedSize, err := snappy.DecodedLen(compressedBytes)
	if err != nil {
		return -1
	}
	return decompressedSize
}

type writeCloser struct {
	i    io.Writer
	pool *sync.Pool
	buff *bytes.Buffer

	dst []byte
}

func (w *writeCloser) Reset(i io.Writer) {
	w.i = i
}

func (w *writeCloser) Write(p []byte) (n int, err error) {
	return w.buff.Write(p)
}

func (w *writeCloser) Close() error {
	defer func() {
		w.buff.Reset()
		w.dst = w.dst[0:cap(w.dst)]
		w.pool.Put(w)
	}()

	if w.i != nil {
		w.dst = snappy.Encode(w.dst, w.buff.Bytes())
		_, err := w.i.Write(w.dst)
		return err
	}

	return nil
}

type reader struct {
	pool  *sync.Pool
	cbuff *bytes.Buffer
	dbuff *bytes.Buffer
	dst   []byte
}

func (r *reader) Reset(ir io.Reader) error {
	_, err := r.cbuff.ReadFrom(ir)

	if err != nil {
		return err
	}

	r.dst, err = snappy.Decode(r.dst, r.cbuff.Bytes())

	if err != nil {
		return err
	}

	r.dbuff = bytes.NewBuffer(r.dst)
	return nil
}

func (r *reader) Read(p []byte) (n int, err error) {
	n, err = r.dbuff.Read(p)
	if err == io.EOF {
		r.cbuff.Reset()
		r.dst = r.dst[0:cap(r.dst)]
		r.pool.Put(r)
	}
	return n, err
}
