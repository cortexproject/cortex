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

type ReadAtWithContextCloser interface {
	io.Closer
	WithContext(ctx context.Context) io.ReaderAt
}

type fileReadAt struct {
	*os.File
}

// NewFileReadAt returns a ReadAtCloserWithContext for reading from a local file.
func NewFileReadAt(f *os.File) ReadAtWithContextCloser {
	return &fileReadAt{
		File: f,
	}
}

func (f *fileReadAt) WithContext(_ context.Context) io.ReaderAt {
	return f.File
}

type bReadAt struct {
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

func (b *bReadAt) WithContext(ctx context.Context) io.ReaderAt {
	return readAtFunc{
		f: func(p []byte, off int64) (n int, err error) {
			rc, err := b.obj.GetRange(ctx, b.path, off, int64(len(p)))
			if err != nil {
				return 0, err
			}
			defer func() { _ = rc.Close() }()
			n, err = io.ReadFull(rc, p)
			if err == io.EOF {
				err = nil
			}
			return
		},
	}
}

func (b *bReadAt) Close() error {
	return nil
}

type readAtFunc struct {
	f func([]byte, int64) (n int, err error)
}

func (r readAtFunc) ReadAt(p []byte, off int64) (n int, err error) {
	return r.f(p, off)
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

func NewOptimisticReaderAt(r io.ReaderAt, minOffset, maxOffset int64) io.ReaderAt {
	if minOffset < maxOffset {
		b := make([]byte, maxOffset-minOffset)
		n, err := r.ReadAt(b, minOffset)
		if err == nil {
			return &optimisticReaderAt{r: r, b: b[:n], offset: minOffset}
		}
	}
	return r
}
