package batch

import (
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/chunk"
	promchunk "github.com/cortexproject/cortex/pkg/chunk/encoding"
)

const (
	fp     = 1
	userID = "0"
	step   = 1 * time.Second
)

func TestChunkIter(t *testing.T) {
	forEncodings(t, func(t *testing.T, enc promchunk.Encoding) {
		chunk := mkChunk(t, 0, 100, enc)
		iter := &chunkIterator{}
		iter.reset(chunk)
		testIter(t, 100, newIteratorAdapter(iter))
		testSeek(t, 100, newIteratorAdapter(iter))
	})
}

func forEncodings(t *testing.T, f func(t *testing.T, enc promchunk.Encoding)) {
	for _, enc := range []promchunk.Encoding{
		promchunk.DoubleDelta, promchunk.Varbit, promchunk.Bigchunk,
	} {
		t.Run(enc.String(), func(t *testing.T) {
			f(t, enc)
		})
	}
}

func mkChunk(t require.TestingT, from model.Time, points int, enc promchunk.Encoding) chunk.Chunk {
	metric := model.Metric{
		model.MetricNameLabel: "foo",
	}
	pc, err := promchunk.NewForEncoding(enc)
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

func TestSeek(t *testing.T) {
	var it mockIterator
	c := chunkIterator{
		chunk: chunk.Chunk{
			Through: promchunk.BatchSize,
		},
		it: &it,
	}

	for i := 0; i < promchunk.BatchSize-1; i++ {
		require.True(t, c.Seek(int64(i), 1))
	}
	require.Equal(t, 1, it.seeks)

	require.True(t, c.Seek(int64(promchunk.BatchSize), 1))
	require.Equal(t, 2, it.seeks)
}

type mockIterator struct {
	seeks int
}

func (i *mockIterator) Scan() bool {
	return true
}

func (i *mockIterator) FindAtOrAfter(model.Time) bool {
	i.seeks++
	return true
}

func (i *mockIterator) Value() model.SamplePair {
	return model.SamplePair{}
}

func (i *mockIterator) Batch(size int) promchunk.Batch {
	batch := promchunk.Batch{
		Length: promchunk.BatchSize,
	}
	for i := 0; i < promchunk.BatchSize; i++ {
		batch.Timestamps[i] = int64(i)
	}
	return batch
}

func (i *mockIterator) Err() error {
	return nil
}
