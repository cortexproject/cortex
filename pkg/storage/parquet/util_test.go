package storage

import (
	"bytes"
	"context"
	"io"
)

type BufferReadAt struct {
	buffer *bytes.Buffer
}

func (b BufferReadAt) ReadAt(p []byte, off int64) (n int, err error) {
	n = copy(p, b.buffer.Bytes()[off:off+int64(len(p))])
	return
}

func (b BufferReadAt) CreateReadAtWithContext(_ context.Context) io.ReaderAt {
	return b
}
