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
	"os"

	"github.com/thanos-io/objstore"
)

type SizeReaderAt interface {
	io.ReaderAt
	Size() int64
}

type ReadAtWithContextCloser interface {
	WithContext(ctx context.Context) SizeReaderAt
	io.Closer
}

type fileReadAt struct {
	ctx context.Context
	*os.File
}

// NewFileReadAt returns a ReadAtCloserWithContext for reading from a local file.
func NewFileReadAt(f *os.File) ReadAtWithContextCloser {
	return &fileReadAt{
		File: f,
	}
}

func (f *fileReadAt) WithContext(ctx context.Context) SizeReaderAt {
	return &fileReadAt{
		ctx:  ctx,
		File: f.File,
	}
}

func (f *fileReadAt) Size() int64 {
	fi, err := f.Stat()
	if err != nil {
		return 0
	}
	return fi.Size()
}

type bReadAt struct {
	ctx  context.Context
	path string
	obj  objstore.BucketReader
}

// NewBucketReadAt returns a ReadAtWithContextCloser for reading from a bucket.
func NewBucketReadAt(path string, obj objstore.BucketReader) ReadAtWithContextCloser {
	return &bReadAt{
		path: path,
		obj:  obj,
	}
}

func (b *bReadAt) WithContext(ctx context.Context) SizeReaderAt {
	return &bReadAt{
		ctx:  ctx,
		path: b.path,
		obj:  b.obj,
	}
}

func (b *bReadAt) Size() int64 {
	attr, err := b.obj.Attributes(context.Background(), b.path)
	if err != nil {
		return 0
	}
	return attr.Size
}

func (b *bReadAt) Close() error {
	return nil
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
	r      SizeReaderAt
	b      []byte
	offset int64
}

func (or optimisticReaderAt) Size() int64 {
	return or.r.Size()
}

func (or optimisticReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= or.offset && off < or.offset+int64(len(or.b)) {
		diff := off - or.offset
		n := copy(p, or.b[diff:])
		return n, nil
	}

	return or.r.ReadAt(p, off)
}

func NewOptimisticReaderAt(r SizeReaderAt, minOffset, maxOffset int64) SizeReaderAt {
	if minOffset < maxOffset {
		b := make([]byte, maxOffset-minOffset)
		n, err := r.ReadAt(b, minOffset)
		if err == nil {
			return &optimisticReaderAt{r: r, b: b[:n], offset: minOffset}
		}
	}
	return r
}
