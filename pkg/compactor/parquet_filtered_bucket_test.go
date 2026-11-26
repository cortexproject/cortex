package compactor

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	cortex_parquet "github.com/cortexproject/cortex/pkg/storage/parquet"
)

var parquetTestFiles = []string{
	"0.labels.parquet",
	"0.chunks.parquet",
	"1.labels.parquet",
	"nested/2.chunks.parquet",
	cortex_parquet.ConverterMarkerFileName,
}

func uploadBlockWithParquetFiles(t *testing.T, bkt objstore.Bucket, id ulid.ULID) {
	t.Helper()

	var meta metadata.Meta
	meta.ULID = id
	meta.MinTime = 0
	meta.MaxTime = 1000
	meta.Version = metadata.TSDBVersion1
	meta.Thanos = metadata.Thanos{Version: metadata.ThanosVersion1, Source: metadata.TestSource}
	var buf bytes.Buffer
	require.NoError(t, meta.Write(&buf))

	files := map[string][]byte{
		block.MetaFilename:  buf.Bytes(),
		block.IndexFilename: []byte("index"),
		"chunks/000001":     []byte("chunk"),
	}
	for _, name := range parquetTestFiles {
		files[name] = []byte("parquet")
	}

	for name, data := range files {
		require.NoError(t, bkt.Upload(context.Background(), id.String()+"/"+name, bytes.NewReader(data)))
	}
}

func TestParquetFilteredBucket_Iter(t *testing.T) {
	ctx := context.Background()
	inmem := objstore.NewInMemBucket()
	id := ulid.MustNew(1, nil)
	uploadBlockWithParquetFiles(t, inmem, id)

	filtered := newParquetFilteredBucket(objstore.WithNoopInstr(inmem))

	listRecursive := func(bkt objstore.BucketReader) []string {
		var names []string
		require.NoError(t, bkt.Iter(ctx, id.String(), func(name string) error {
			names = append(names, name)
			return nil
		}, objstore.WithRecursiveIter()))
		return names
	}

	// Control: the unfiltered bucket lists the parquet files.
	unfiltered := listRecursive(inmem)
	for _, name := range parquetTestFiles {
		require.Contains(t, unfiltered, id.String()+"/"+name)
	}

	got := listRecursive(filtered)
	for _, name := range parquetTestFiles {
		require.NotContains(t, got, id.String()+"/"+name)
	}
	require.Contains(t, got, id.String()+"/"+block.MetaFilename)
	require.Contains(t, got, id.String()+"/"+block.IndexFilename)
	require.Contains(t, got, id.String()+"/chunks/000001")

	// Same behavior for IterWithAttributes.
	var gotAttrs []string
	require.NoError(t, filtered.IterWithAttributes(ctx, id.String(), func(attrs objstore.IterObjectAttributes) error {
		gotAttrs = append(gotAttrs, attrs.Name)
		return nil
	}, objstore.WithRecursiveIter()))
	for _, name := range parquetTestFiles {
		require.NotContains(t, gotAttrs, id.String()+"/"+name)
	}
	require.Contains(t, gotAttrs, id.String()+"/"+block.IndexFilename)
}

func TestParquetFilteredBucket_ReadsAreNotFiltered(t *testing.T) {
	ctx := context.Background()
	inmem := objstore.NewInMemBucket()
	id := ulid.MustNew(1, nil)
	uploadBlockWithParquetFiles(t, inmem, id)

	filtered := newParquetFilteredBucket(objstore.WithNoopInstr(inmem))

	for _, name := range parquetTestFiles {
		exists, err := filtered.Exists(ctx, id.String()+"/"+name)
		require.NoError(t, err)
		require.True(t, exists, name)
	}

	// The filter survives re-wrapping for expected errors.
	rewrapped := filtered.WithExpectedErrs(filtered.IsObjNotFoundErr)
	var names []string
	require.NoError(t, rewrapped.Iter(ctx, id.String(), func(name string) error {
		names = append(names, name)
		return nil
	}, objstore.WithRecursiveIter()))
	require.NotContains(t, names, id.String()+"/0.labels.parquet")
	require.Contains(t, names, id.String()+"/"+block.IndexFilename)
}

func TestParquetFilteredBucket_BlockDownloadSkipsParquetFiles(t *testing.T) {
	ctx := context.Background()
	inmem := objstore.NewInMemBucket()
	id := ulid.MustNew(1, nil)
	uploadBlockWithParquetFiles(t, inmem, id)

	// Control: downloading from the unfiltered bucket pulls the parquet files.
	controlDir := t.TempDir()
	require.NoError(t, block.Download(ctx, log.NewNopLogger(), inmem, id, controlDir))
	require.FileExists(t, filepath.Join(controlDir, "0.labels.parquet"))
	require.FileExists(t, filepath.Join(controlDir, cortex_parquet.ConverterMarkerFileName))

	filtered := newParquetFilteredBucket(objstore.WithNoopInstr(inmem))
	dst := t.TempDir()
	require.NoError(t, block.Download(ctx, log.NewNopLogger(), filtered, id, dst))

	require.FileExists(t, filepath.Join(dst, block.MetaFilename))
	require.FileExists(t, filepath.Join(dst, block.IndexFilename))
	require.FileExists(t, filepath.Join(dst, block.ChunksDirname, "000001"))

	for _, name := range parquetTestFiles {
		require.NoFileExists(t, filepath.Join(dst, name))
	}
}
