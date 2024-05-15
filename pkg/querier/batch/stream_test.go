package batch

import (
	"strconv"
	"testing"
	"unsafe"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	promchunk "github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/encoding"
)

func TestStream(t *testing.T) {
	t.Parallel()
	forEncodings(t, func(t *testing.T, enc encoding.Encoding) {
		for i, tc := range []struct {
			input1, input2 []promchunk.Batch
			output         batchStream
		}{
			{
				input1: []promchunk.Batch{mkBatch(0, enc)},
				output: []promchunk.Batch{mkBatch(0, enc)},
			},

			{
				input1: []promchunk.Batch{mkBatch(0, enc)},
				input2: []promchunk.Batch{mkBatch(0, enc)},
				output: []promchunk.Batch{mkBatch(0, enc)},
			},

			{
				input1: []promchunk.Batch{mkBatch(0, enc)},
				input2: []promchunk.Batch{mkBatch(promchunk.BatchSize, enc)},
				output: []promchunk.Batch{mkBatch(0, enc), mkBatch(promchunk.BatchSize, enc)},
			},

			{
				input1: []promchunk.Batch{mkBatch(0, enc), mkBatch(promchunk.BatchSize, enc)},
				input2: []promchunk.Batch{mkBatch(promchunk.BatchSize/2, enc), mkBatch(2*promchunk.BatchSize, enc)},
				output: []promchunk.Batch{mkBatch(0, enc), mkBatch(promchunk.BatchSize, enc), mkBatch(2*promchunk.BatchSize, enc)},
			},

			{
				input1: []promchunk.Batch{mkBatch(promchunk.BatchSize/2, enc), mkBatch(3*promchunk.BatchSize/2, enc), mkBatch(5*promchunk.BatchSize/2, enc)},
				input2: []promchunk.Batch{mkBatch(0, enc), mkBatch(promchunk.BatchSize, enc), mkBatch(3*promchunk.BatchSize, enc)},
				output: []promchunk.Batch{mkBatch(0, enc), mkBatch(promchunk.BatchSize, enc), mkBatch(2*promchunk.BatchSize, enc), mkBatch(3*promchunk.BatchSize, enc)},
			},
		} {
			tc := tc
			t.Run(strconv.Itoa(i), func(t *testing.T) {
				t.Parallel()
				result := make(batchStream, len(tc.input1)+len(tc.input2))
				result = mergeStreams(tc.input1, tc.input2, result, promchunk.BatchSize)
				require.Equal(t, len(tc.output), len(result))
				for j := 0; j < len(tc.output); j++ {
					require.Equal(t, tc.output[j].ValType, result[j].ValType)
					require.Equal(t, tc.output[j].Length, result[j].Length)
					require.Equal(t, tc.output[j].Index, result[j].Index)
					require.Equal(t, tc.output[j].Timestamps, result[j].Timestamps)
					require.Equal(t, tc.output[j].Values, result[j].Values)
					for k := 0; k < len(tc.output[j].HistogramValues); k++ {
						switch tc.output[j].ValType {
						case chunkenc.ValHistogram:
							require.Equal(t, (*histogram.Histogram)(tc.output[j].HistogramValues[k]), (*histogram.Histogram)(result[j].HistogramValues[k]))
						case chunkenc.ValFloatHistogram:
							require.Equal(t, (*histogram.FloatHistogram)(tc.output[j].HistogramValues[k]), (*histogram.FloatHistogram)(result[j].HistogramValues[k]))
						default:
						}
					}
				}
			})
		}
	})
}

func mkBatch(from int64, enc encoding.Encoding) promchunk.Batch {
	var result promchunk.Batch
	for i := int64(0); i < promchunk.BatchSize; i++ {
		result.Timestamps[i] = from + i
		switch enc {
		case encoding.PrometheusXorChunk:
			result.Values[i] = float64(from + i)
		case encoding.PrometheusHistogramChunk:
			result.HistogramValues[i] = unsafe.Pointer(testHistogram(int(from+i), 5, 20))
		case encoding.PrometheusFloatHistogramChunk:
			result.HistogramValues[i] = unsafe.Pointer(testHistogram(int(from+i), 5, 20).ToFloat(nil))
		}
	}
	result.Length = promchunk.BatchSize
	result.ValType = enc.ChunkValueType()
	return result
}

func testHistogram(count, numSpans, numBuckets int) *histogram.Histogram {
	bucketsPerSide := numBuckets / 2
	spanLength := uint32(bucketsPerSide / numSpans)
	h := &histogram.Histogram{
		CounterResetHint: histogram.GaugeType,
		Count:            uint64(count),
		ZeroCount:        uint64(count),
		ZeroThreshold:    1e-128,
		Sum:              18.4 * float64(count+1),
		Schema:           2,
		NegativeSpans:    make([]histogram.Span, numSpans),
		PositiveSpans:    make([]histogram.Span, numSpans),
		NegativeBuckets:  make([]int64, bucketsPerSide),
		PositiveBuckets:  make([]int64, bucketsPerSide),
	}
	for j := 0; j < numSpans; j++ {
		s := histogram.Span{Offset: 1, Length: spanLength}
		h.NegativeSpans[j] = s
		h.PositiveSpans[j] = s
	}

	for j := 0; j < bucketsPerSide; j++ {
		h.NegativeBuckets[j] = 1
		h.PositiveBuckets[j] = 1
	}
	return h
}
