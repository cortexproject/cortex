// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"
	"io"

	"github.com/thanos-io/objstore"
)

type ReadAtWithContext interface {
	io.ReaderAt
	WithContext(ctx context.Context) io.ReaderAt
}

type bReadAt struct {
	path string
	obj  objstore.Bucket
	ctx  context.Context
}

func NewBucketReadAt(ctx context.Context, path string, obj objstore.Bucket) ReadAtWithContext {
	return &bReadAt{
		path: path,
		obj:  obj,
		ctx:  ctx,
	}
}

func (b *bReadAt) WithContext(ctx context.Context) io.ReaderAt {
	return &bReadAt{
		path: b.path,
		obj:  b.obj,
		ctx:  ctx,
	}
}

func (b *bReadAt) ReadAt(p []byte, off int64) (n int, err error) {
	rc, err := b.obj.GetRange(b.ctx, b.path, off, int64(len(p)))
	if err != nil {
		return 0, err
	}
	defer func() { _ = rc.Close() }()
	n, err = io.ReadFull(rc, p)
	if err == io.EOF {
		err = nil
	}
	return
}

type optimisticReaderAt struct {
	r      io.ReaderAt
	b      []byte
	offset int64
}

func (b optimisticReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= b.offset && off < b.offset+int64(len(b.b)) {
		diff := off - b.offset
		n := copy(p, b.b[diff:])
		return n, nil
	}

	return b.r.ReadAt(p, off)
}

func newOptimisticReaderAt(r io.ReaderAt, minOffset, maxOffset int64) io.ReaderAt {
	if minOffset < maxOffset {
		b := make([]byte, maxOffset-minOffset)
		n, err := r.ReadAt(b, minOffset)
		if err == nil {
			return &optimisticReaderAt{r: r, b: b[:n], offset: minOffset}
		}
	}
	return r
}
