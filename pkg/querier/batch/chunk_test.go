package batch

import (
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/chunk"
	promchunk "github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/util"
	histogram_util "github.com/cortexproject/cortex/pkg/util/histogram"
)

const (
	step = 1 * time.Second
)

func TestChunkIter(t *testing.T) {
	t.Parallel()
	forEncodings(t, func(t *testing.T, enc promchunk.Encoding) {
		chunk := mkGenericChunk(t, 0, 100, enc)
		iter := &chunkIterator{}

		iter.reset(chunk)
		testIter(t, 100, newIteratorAdapter(iter), enc)

		iter.reset(chunk)
		testSeek(t, 100, newIteratorAdapter(iter), enc)
	})
}

func forEncodings(t *testing.T, f func(t *testing.T, enc promchunk.Encoding)) {
	for _, enc := range []promchunk.Encoding{
		//promchunk.PrometheusXorChunk,
		promchunk.PrometheusHistogramChunk,
		//promchunk.PrometheusFloatHistogramChunk,
	} {
		enc := enc
		t.Run(enc.String(), func(t *testing.T) {
			t.Parallel()
			f(t, enc)
		})
	}
}

func mkGenericChunk(t require.TestingT, from model.Time, points int, enc promchunk.Encoding) GenericChunk {
	ck := util.GenerateChunk(t, step, from, points, enc)
	return NewGenericChunk(int64(ck.From), int64(ck.Through), ck.NewIterator)
}

func testIter(t require.TestingT, points int, iter chunkenc.Iterator, enc promchunk.Encoding) {
	histograms := histogram_util.GenerateTestHistograms(0, 1000, points)
	ets := model.TimeFromUnix(0)
	for i := 0; i < points; i++ {
		require.Equal(t, iter.Next(), enc.ChunkValueType(), strconv.Itoa(i))
		switch enc {
		case promchunk.PrometheusXorChunk:
			ts, v := iter.At()
			require.EqualValues(t, int64(ets), ts, strconv.Itoa(i))
			require.EqualValues(t, float64(ets), v, strconv.Itoa(i))
		case promchunk.PrometheusHistogramChunk:
			ts, v := iter.AtHistogram(nil)
			require.EqualValues(t, int64(ets), ts, strconv.Itoa(i))
			require.EqualValues(t, histograms[i], v, strconv.Itoa(i))
		case promchunk.PrometheusFloatHistogramChunk:
			ts, v := iter.AtFloatHistogram(nil)
			require.EqualValues(t, int64(ets), ts, strconv.Itoa(i))
			require.EqualValues(t, histograms[i].ToFloat(nil), v, strconv.Itoa(i))
		}
		ets = ets.Add(step)
	}
	require.Equal(t, iter.Next(), chunkenc.ValNone)
}

func testSeek(t require.TestingT, points int, iter chunkenc.Iterator, enc promchunk.Encoding) {
	histograms := histogram_util.GenerateTestHistograms(0, 1000, points)
	for i := 0; i < points; i += points / 10 {
		ets := int64(i * int(step/time.Millisecond))

		require.Equal(t, iter.Seek(ets), enc.ChunkValueType(), strconv.Itoa(i))

		switch enc {
		case promchunk.PrometheusXorChunk:
			ts, v := iter.At()
			require.EqualValues(t, ets, ts, strconv.Itoa(i))
			require.EqualValues(t, float64(ets), v, strconv.Itoa(i))
		case promchunk.PrometheusHistogramChunk:
			ts, v := iter.AtHistogram(nil)
			require.EqualValues(t, ets, ts, strconv.Itoa(i))
			require.EqualValues(t, histograms[i], v, strconv.Itoa(i))
		case promchunk.PrometheusFloatHistogramChunk:
			ts, v := iter.AtFloatHistogram(nil)
			require.EqualValues(t, ets, ts, strconv.Itoa(i))
			require.EqualValues(t, histograms[i].ToFloat(nil), v, strconv.Itoa(i))
		}
		require.NoError(t, iter.Err())

		for j := i + 1; j < i+points/10; j++ {
			ets := int64(j * int(step/time.Millisecond))
			require.Equal(t, iter.Next(), enc.ChunkValueType(), strconv.Itoa(i))

			switch enc {
			case promchunk.PrometheusXorChunk:
				ts, v := iter.At()
				require.EqualValues(t, ets, ts)
				require.EqualValues(t, float64(ets), v)
			case promchunk.PrometheusHistogramChunk:
				ts, v := iter.AtHistogram(nil)
				require.EqualValues(t, ets, ts)
				require.EqualValues(t, histograms[j], v)
			case promchunk.PrometheusFloatHistogramChunk:
				ts, v := iter.AtFloatHistogram(nil)
				require.EqualValues(t, ets, ts)
				require.EqualValues(t, histograms[j].ToFloat(nil), v)
			}
			require.NoError(t, iter.Err())
		}
	}
}

func TestSeek(t *testing.T) {
	t.Parallel()
	var it mockIterator
	c := chunkIterator{
		chunk: GenericChunk{
			MaxTime: chunk.BatchSize,
		},
		it: &it,
	}

	for i := 0; i < chunk.BatchSize-1; i++ {
		require.Equal(t, chunkenc.ValFloat, c.Seek(int64(i), 1))
	}
	require.Equal(t, 1, it.seeks)

	require.Equal(t, chunkenc.ValFloat, c.Seek(int64(chunk.BatchSize), 1))
	require.Equal(t, 2, it.seeks)
}

type mockIterator struct {
	seeks int
}

func (i *mockIterator) Scan() chunkenc.ValueType {
	return chunkenc.ValFloat
}

func (i *mockIterator) FindAtOrAfter(model.Time) chunkenc.ValueType {
	i.seeks++
	return chunkenc.ValFloat
}

func (i *mockIterator) Batch(size int, valType chunkenc.ValueType) chunk.Batch {
	batch := chunk.Batch{
		Length:  chunk.BatchSize,
		ValType: valType,
	}
	for i := 0; i < chunk.BatchSize; i++ {
		batch.Timestamps[i] = int64(i)
	}
	return batch
}

func (i *mockIterator) Err() error {
	return nil
}
