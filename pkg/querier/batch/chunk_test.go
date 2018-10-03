package batch

import (
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/chunk"
	promchunk "github.com/cortexproject/cortex/pkg/prom1/storage/local/chunk"
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
	testIter(t, 100, newIteratorAdapter(iter))
	testSeek(t, 100, newIteratorAdapter(iter))
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

func testIter(t require.TestingT, points int, iter storage.SeriesIterator) {
	ets := model.TimeFromUnix(0)
	for i := 0; i < points; i++ {
		require.True(t, iter.Next(), strconv.Itoa(i))
		ts, v := iter.At()
		require.EqualValues(t, int64(ets), ts, strconv.Itoa(i))
		require.EqualValues(t, float64(ets), v, strconv.Itoa(i))
		ets = ets.Add(step)
	}
	require.False(t, iter.Next())
}

func testSeek(t require.TestingT, points int, iter storage.SeriesIterator) {
	for i := 0; i < points; i += points / 10 {
		ets := int64(i * int(step/time.Millisecond))

		require.True(t, iter.Seek(ets))
		ts, v := iter.At()
		require.EqualValues(t, ets, ts)
		require.EqualValues(t, v, float64(ets))
		require.NoError(t, iter.Err())

		for j := i + 1; j < i+points/10; j++ {
			ets := int64(j * int(step/time.Millisecond))
			require.True(t, iter.Next())
			ts, v := iter.At()
			require.EqualValues(t, ets, ts)
			require.EqualValues(t, float64(ets), v)
			require.NoError(t, iter.Err())
		}
	}
}
