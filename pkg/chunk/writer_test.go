package chunk

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/test"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/util/flagext"
)

func TestWriter(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), userID)
	now := model.Now()
	chunk1, chunk2, _, _ := dummyChunks(now)

	st := newTestChunkStore(t, "v6")
	defer st.Stop()
	store := st.(CompositeStore).stores[0].Store.(*store)

	var cfg WriterConfig
	flagext.DefaultValues(&cfg)
	writer := NewWriter(cfg, store.index, store.chunks)

	sendChunk := func(chunk Chunk) {
		// can't use writer to store chunks with mock storage as it isn't flexible enough
		err := store.storage.PutChunks(ctx, []Chunk{chunk})
		require.NoError(t, err)
		writeReqs, err := store.calculateIndexEntries(userID, chunk.From, chunk.Through, chunk)
		require.NoError(t, err)
		writer.Write <- writeReqs
	}

	sendChunk(chunk1)
	sendChunk(chunk2)

	// Ensure chunks are flushed to store
	writer.Stop()

	// Now read back the data
	matchers, err := promql.ParseMetricSelector("foo")
	require.NoError(t, err)
	chunks, err := store.Get(ctx, now.Add(-time.Hour), now, matchers...)
	require.NoError(t, err)

	expect := []Chunk{chunk1, chunk2}
	if !reflect.DeepEqual(expect, chunks) {
		t.Fatalf("wrong chunks - %s", test.Diff(expect, chunks))
	}
}
