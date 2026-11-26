package compactor

import (
	"context"
	"path"
	"strings"

	"github.com/thanos-io/objstore"

	cortex_parquet "github.com/cortexproject/cortex/pkg/storage/parquet"
)

// parquetFileSuffix is the extension of the parquet files written by the parquet converter.
const parquetFileSuffix = ".parquet"

// parquetFilteredBucket hides the files produced by the parquet converter from
// listings. The converter writes them under the same prefix as the TSDB block,
// and block.Download lists and downloads everything under that prefix, although
// compaction never reads them. Only listings are filtered: reads, uploads and
// deletes behave as usual.
type parquetFilteredBucket struct {
	objstore.Bucket
}

// newParquetFilteredBucket wraps the bucket used for compaction so that parquet
// files and the parquet converter markers are not listed.
func newParquetFilteredBucket(b objstore.InstrumentedBucket) objstore.InstrumentedBucket {
	return &parquetFilteredBucket{Bucket: b}
}

func isParquetObject(name string) bool {
	if strings.HasSuffix(name, objstore.DirDelim) {
		return false
	}
	base := path.Base(name)
	return strings.HasSuffix(name, parquetFileSuffix) ||
		base == cortex_parquet.ConverterMarkerFileName ||
		base == cortex_parquet.NoConvertMarkerFileName
}

func (b *parquetFilteredBucket) Iter(ctx context.Context, dir string, f func(name string) error, options ...objstore.IterOption) error {
	return b.Bucket.Iter(ctx, dir, func(name string) error {
		if isParquetObject(name) {
			return nil
		}
		return f(name)
	}, options...)
}

func (b *parquetFilteredBucket) IterWithAttributes(ctx context.Context, dir string, f func(attrs objstore.IterObjectAttributes) error, options ...objstore.IterOption) error {
	return b.Bucket.IterWithAttributes(ctx, dir, func(attrs objstore.IterObjectAttributes) error {
		if isParquetObject(attrs.Name) {
			return nil
		}
		return f(attrs)
	}, options...)
}

// WithExpectedErrs keeps the filtering when the underlying bucket is re-wrapped.
func (b *parquetFilteredBucket) WithExpectedErrs(fn objstore.IsOpFailureExpectedFunc) objstore.Bucket {
	if ib, ok := b.Bucket.(objstore.InstrumentedBucket); ok {
		return &parquetFilteredBucket{Bucket: ib.WithExpectedErrs(fn)}
	}
	return b
}

// ReaderWithExpectedErrs keeps the filtering when the underlying bucket is re-wrapped.
func (b *parquetFilteredBucket) ReaderWithExpectedErrs(fn objstore.IsOpFailureExpectedFunc) objstore.BucketReader {
	return b.WithExpectedErrs(fn)
}
