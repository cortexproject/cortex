package util

import (
	"context"
	"io"

	"github.com/thanos-io/objstore"
)

type bReadAt struct {
	path string
	obj  objstore.Bucket
	ctx  context.Context
}

func NewBucketReadAt(ctx context.Context, path string, obj objstore.Bucket) io.ReaderAt {
	return &bReadAt{
		path: path,
		obj:  obj,
		ctx:  ctx,
	}
}

func (b *bReadAt) ReadAt(p []byte, off int64) (n int, err error) {
	rc, err := b.obj.GetRange(b.ctx, b.path, off, int64(len(p)))
	if err != nil {
		return 0, err
	}
	defer func() { _ = rc.Close() }()
	n, err = rc.Read(p)
	if err == io.EOF {
		err = nil
	}
	return
}
