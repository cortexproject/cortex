package compactor

import (
	"context"
	"io"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	cortex_parquet "github.com/cortexproject/cortex/pkg/storage/parquet"
	"github.com/cortexproject/cortex/pkg/util/services"
)

// bucketReadRecorder records the names of the objects read from a bucket.
type bucketReadRecorder struct {
	mtx   sync.Mutex
	reads []string
}

func (r *bucketReadRecorder) record(name string) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.reads = append(r.reads, name)
}

func (r *bucketReadRecorder) names() []string {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return append([]string(nil), r.reads...)
}

// readRecordingBucket records every object read through Get and GetRange.
type readRecordingBucket struct {
	objstore.Bucket
	rec *bucketReadRecorder
}

func newReadRecordingBucket(b objstore.Bucket, rec *bucketReadRecorder) objstore.InstrumentedBucket {
	return &readRecordingBucket{Bucket: b, rec: rec}
}

func (b *readRecordingBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	b.rec.record(name)
	return b.Bucket.Get(ctx, name)
}

func (b *readRecordingBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	b.rec.record(name)
	return b.Bucket.GetRange(ctx, name, off, length)
}

func (b *readRecordingBucket) WithExpectedErrs(fn objstore.IsOpFailureExpectedFunc) objstore.Bucket {
	if ib, ok := b.Bucket.(objstore.InstrumentedBucket); ok {
		return &readRecordingBucket{Bucket: ib.WithExpectedErrs(fn), rec: b.rec}
	}
	return b
}

func (b *readRecordingBucket) ReaderWithExpectedErrs(fn objstore.IsOpFailureExpectedFunc) objstore.BucketReader {
	return b.WithExpectedErrs(fn)
}

func TestCompactor_ShouldNotDownloadParquetFilesWhenCompacting(t *testing.T) {
	rec := &bucketReadRecorder{}
	bucketClient := newReadRecordingBucket(objstore.WithNoopInstr(objstore.NewInMemBucket()), rec)

	blocks := []ulid.ULID{
		createTSDBBlock(t, bucketClient, "user-1", 10, 20, map[string]string{"__name__": "Teste"}),
		createTSDBBlock(t, bucketClient, "user-1", 20, 30, map[string]string{"__name__": "Teste"}),
	}

	// Put the parquet converter output next to each block.
	for _, id := range blocks {
		for _, name := range parquetTestFiles {
			require.NoError(t, bucketClient.Upload(context.Background(), path.Join("user-1", id.String(), name), strings.NewReader("parquet")))
		}
	}

	c, tsdbCompactor, tsdbPlanner, _, _ := prepare(t, prepareConfig(), bucketClient, nil)

	// The TSDB compaction only runs once the blocks are on disk, so interrupt
	// the run from there.
	ctx, cancel := context.WithCancel(context.Background())
	tsdbCompactor.On("CompactWithBlockPopulator", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]ulid.ULID{}, context.Canceled).Run(func(args mock.Arguments) {
		cancel()
	})
	tsdbPlanner.On("Plan", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*metadata.Meta{
		{
			ULID:    blocks[0],
			MinTime: 10,
			MaxTime: 20,
		},
		{
			ULID:    blocks[1],
			MinTime: 20,
			MaxTime: 30,
		},
	}, nil)

	require.NoError(t, services.StartAndAwaitRunning(ctx, c))
	require.Eventually(t, func() bool {
		return prom_testutil.ToFloat64(c.CompactionRunsInterrupted) == 1
	}, 20*time.Second, 50*time.Millisecond)
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))

	tsdbCompactor.AssertCalled(t, "CompactWithBlockPopulator", mock.Anything, mock.Anything, mock.Anything, mock.Anything)

	reads := rec.names()

	// The compaction inputs were really downloaded.
	for _, id := range blocks {
		prefix := path.Join("user-1", id.String()) + "/"
		require.Contains(t, reads, prefix+"index")

		hasChunks := false
		for _, name := range reads {
			if strings.HasPrefix(name, prefix+"chunks/") {
				hasChunks = true
			}
		}
		require.True(t, hasChunks, "no chunk file of block %s was read", id)
	}

	// None of the parquet converter files were.
	for _, name := range reads {
		require.False(t, strings.HasSuffix(name, ".parquet"), name)
		require.NotEqual(t, cortex_parquet.ConverterMarkerFileName, path.Base(name), name)
		require.NotEqual(t, cortex_parquet.NoConvertMarkerFileName, path.Base(name), name)
	}
}
