package batch

import (
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/chunk"
	promchunk "github.com/cortexproject/cortex/pkg/chunk/encoding"
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

func mkChunk(t require.TestingT, step time.Duration, from model.Time, points int, enc promchunk.Encoding) chunk.Chunk {
	metric := labels.Labels{
		{Name: model.MetricNameLabel, Value: "foo"},
	}
	pe := enc.PromChunkEncoding()
	pc, err := chunkenc.NewEmptyChunk(pe)
	require.NoError(t, err)
	appender, err := pc.Appender()
	require.NoError(t, err)
	ts := from

	switch pe {
	case chunkenc.EncXOR:
		for i := 0; i < points; i++ {
			appender.Append(int64(ts), float64(ts))
			ts = ts.Add(step)
		}
	case chunkenc.EncHistogram:
		histograms := histogram_util.GenerateTestHistograms(int(from), int(step/time.Millisecond), points, 5, 20)
		for i := 0; i < points; i++ {
			_, _, appender, err = appender.AppendHistogram(nil, int64(ts), histograms[i], true)
			require.NoError(t, err)
			ts = ts.Add(step)
		}
	case chunkenc.EncFloatHistogram:
		histograms := histogram_util.GenerateTestHistograms(int(from), int(step/time.Millisecond), points, 5, 20)
		for i := 0; i < points; i++ {
			_, _, appender, err = appender.AppendFloatHistogram(nil, int64(ts), histograms[i].ToFloat(nil), true)
			require.NoError(t, err)
			ts = ts.Add(step)
		}
	}

	ts = ts.Add(-step) // undo the add that we did just before exiting the loop
	return chunk.NewChunk(metric, pc, from, ts)
}

func mkGenericChunk(t require.TestingT, from model.Time, points int, enc promchunk.Encoding) GenericChunk {
	ck := mkChunk(t, step, from, points, enc)
	return NewGenericChunk(int64(ck.From), int64(ck.Through), ck.NewIterator)
}

func testIter(t require.TestingT, points int, iter chunkenc.Iterator, enc promchunk.Encoding) {
	histograms := histogram_util.GenerateTestHistograms(0, 1000, points, 5, 20)
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
	histograms := histogram_util.GenerateTestHistograms(0, 1000, points, 5, 20)
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
