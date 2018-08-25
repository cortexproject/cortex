package batch

import (
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/weaveworks/cortex/pkg/chunk"
	promchunk "github.com/weaveworks/cortex/pkg/prom1/storage/local/chunk"
)

const (
	fp     = 1
	userID = "0"
	step   = 1 * time.Second
)

func TestChunkIter(t *testing.T) {
	chunk := mkChunk(t, 0, 100)
	iter := &chunkIterator{}
	iter.reset(chunk)
	testIter(t, 0, 100, newBatchIteratorAdapter(iter))
}

func mkChunk(t require.TestingT, from model.Time, points int) chunk.Chunk {
	metric := model.Metric{
		model.MetricNameLabel: "foo",
	}
	pc, err := promchunk.NewForEncoding(promchunk.DoubleDelta)
	require.NoError(t, err)
	ts := from
	for i := 0; i < points; i++ {
		pcs, err := pc.Add(model.SamplePair{
			Timestamp: ts,
			Value:     model.SampleValue(float64(ts)),
		})
		require.NoError(t, err)
		require.Len(t, pcs, 1)
		pc = pcs[0]
		ts = ts.Add(step)
	}
	return chunk.NewChunk(userID, fp, metric, pc, model.Time(0), ts)
}

func testIter(t require.TestingT, from model.Time, points int, iter storage.SeriesIterator) {
	ets := from
	for i := 0; i < points; i++ {
		require.True(t, iter.Next())
		ts, v := iter.At()
		require.EqualValues(t, int64(ets), ts, strconv.Itoa(i))
		require.EqualValues(t, float64(ets), v, strconv.Itoa(i))
		ets = ets.Add(step)
	}
	require.False(t, iter.Next())
}
