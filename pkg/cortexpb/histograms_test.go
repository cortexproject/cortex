package cortexpb

import (
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsFloatHistogram(t *testing.T) {
	t.Run("integer histogram", func(t *testing.T) {
		h := Histogram{
			Count:     &Histogram_CountInt{CountInt: 10},
			ZeroCount: &Histogram_ZeroCountInt{ZeroCountInt: 1},
		}
		assert.False(t, h.IsFloatHistogram())
	})

	t.Run("float histogram", func(t *testing.T) {
		h := Histogram{
			Count:     &Histogram_CountFloat{CountFloat: 10.5},
			ZeroCount: &Histogram_ZeroCountFloat{ZeroCountFloat: 1.5},
		}
		assert.True(t, h.IsFloatHistogram())
	})

	t.Run("zero value histogram", func(t *testing.T) {
		h := Histogram{}
		assert.False(t, h.IsFloatHistogram())
	})
}

func TestHistogramToHistogramProtoRoundTrip(t *testing.T) {
	original := &histogram.Histogram{
		CounterResetHint: histogram.GaugeType,
		Schema:           3,
		ZeroThreshold:    0.001,
		ZeroCount:        5,
		Count:            30,
		Sum:              19.4,
		PositiveSpans:    []histogram.Span{{Offset: 0, Length: 2}, {Offset: 1, Length: 1}},
		PositiveBuckets:  []int64{1, 2, -1},
		NegativeSpans:    []histogram.Span{{Offset: 1, Length: 1}},
		NegativeBuckets:  []int64{3},
		CustomValues:     []float64{1.0, 2.0},
	}

	timestamp := int64(1234567890)
	proto := HistogramToHistogramProto(timestamp, original)
	result := HistogramProtoToHistogram(proto)

	assert.Equal(t, original.CounterResetHint, result.CounterResetHint)
	assert.Equal(t, original.Schema, result.Schema)
	assert.Equal(t, original.ZeroThreshold, result.ZeroThreshold)
	assert.Equal(t, original.ZeroCount, result.ZeroCount)
	assert.Equal(t, original.Count, result.Count)
	assert.Equal(t, original.Sum, result.Sum)
	assert.Equal(t, original.PositiveSpans, result.PositiveSpans)
	assert.Equal(t, original.PositiveBuckets, result.PositiveBuckets)
	assert.Equal(t, original.NegativeSpans, result.NegativeSpans)
	assert.Equal(t, original.NegativeBuckets, result.NegativeBuckets)
	assert.Equal(t, original.CustomValues, result.CustomValues)
	assert.Equal(t, timestamp, proto.TimestampMs)
}

func TestFloatHistogramToHistogramProtoRoundTrip(t *testing.T) {
	original := &histogram.FloatHistogram{
		CounterResetHint: histogram.NotCounterReset,
		Schema:           5,
		ZeroThreshold:    0.01,
		ZeroCount:        2.5,
		Count:            20.5,
		Sum:              35.7,
		PositiveSpans:    []histogram.Span{{Offset: 0, Length: 3}},
		PositiveBuckets:  []float64{1.5, 2.5, 3.5},
		NegativeSpans:    []histogram.Span{{Offset: 2, Length: 2}},
		NegativeBuckets:  []float64{0.5, 1.5},
		CustomValues:     []float64{10.0},
	}

	timestamp := int64(9876543210)
	proto := FloatHistogramToHistogramProto(timestamp, original)
	result := FloatHistogramProtoToFloatHistogram(proto)

	assert.Equal(t, original.CounterResetHint, result.CounterResetHint)
	assert.Equal(t, original.Schema, result.Schema)
	assert.Equal(t, original.ZeroThreshold, result.ZeroThreshold)
	assert.Equal(t, original.ZeroCount, result.ZeroCount)
	assert.Equal(t, original.Count, result.Count)
	assert.Equal(t, original.Sum, result.Sum)
	assert.Equal(t, original.PositiveSpans, result.PositiveSpans)
	assert.Equal(t, original.PositiveBuckets, result.PositiveBuckets)
	assert.Equal(t, original.NegativeSpans, result.NegativeSpans)
	assert.Equal(t, original.NegativeBuckets, result.NegativeBuckets)
	assert.Equal(t, original.CustomValues, result.CustomValues)
	assert.Equal(t, timestamp, proto.TimestampMs)
}

func TestHistogramProtoToHistogramPanicsOnFloat(t *testing.T) {
	floatProto := FloatHistogramToHistogramProto(0, &histogram.FloatHistogram{
		Count:     10.5,
		ZeroCount: 1.5,
	})

	assert.Panics(t, func() {
		HistogramProtoToHistogram(floatProto)
	})
}

func TestFloatHistogramProtoToFloatHistogramPanicsOnInt(t *testing.T) {
	intProto := HistogramToHistogramProto(0, &histogram.Histogram{
		Count:     10,
		ZeroCount: 1,
	})

	assert.Panics(t, func() {
		FloatHistogramProtoToFloatHistogram(intProto)
	})
}

func TestHistogramPromProtoToHistogramProto(t *testing.T) {
	t.Run("integer histogram", func(t *testing.T) {
		promProto := prompb.Histogram{
			Count:          &prompb.Histogram_CountInt{CountInt: 15},
			Sum:            42.5,
			Schema:         3,
			ZeroThreshold:  0.001,
			ZeroCount:      &prompb.Histogram_ZeroCountInt{ZeroCountInt: 2},
			NegativeSpans:  []prompb.BucketSpan{{Offset: 0, Length: 2}},
			NegativeDeltas: []int64{1, -1},
			PositiveSpans:  []prompb.BucketSpan{{Offset: 1, Length: 3}},
			PositiveDeltas: []int64{1, 2, -2},
			ResetHint:      prompb.Histogram_GAUGE,
			Timestamp:      1000,
			CustomValues:   []float64{5.0},
		}

		result := HistogramPromProtoToHistogramProto(promProto)

		assert.Equal(t, uint64(15), result.GetCountInt())
		assert.Equal(t, 42.5, result.Sum)
		assert.Equal(t, int32(3), result.Schema)
		assert.Equal(t, 0.001, result.ZeroThreshold)
		assert.Equal(t, uint64(2), result.GetZeroCountInt())
		assert.Equal(t, []int64{1, -1}, result.NegativeDeltas)
		assert.Equal(t, []int64{1, 2, -2}, result.PositiveDeltas)
		assert.Equal(t, Histogram_GAUGE, result.ResetHint)
		assert.Equal(t, int64(1000), result.TimestampMs)
		assert.Equal(t, []float64{5.0}, result.CustomValues)
		require.Len(t, result.NegativeSpans, 1)
		assert.Equal(t, int32(0), result.NegativeSpans[0].Offset)
		assert.Equal(t, uint32(2), result.NegativeSpans[0].Length)
		require.Len(t, result.PositiveSpans, 1)
		assert.Equal(t, int32(1), result.PositiveSpans[0].Offset)
		assert.Equal(t, uint32(3), result.PositiveSpans[0].Length)
		assert.False(t, result.IsFloatHistogram())
	})

	t.Run("float histogram", func(t *testing.T) {
		promProto := prompb.Histogram{
			Count:          &prompb.Histogram_CountFloat{CountFloat: 20.5},
			Sum:            100.1,
			Schema:         5,
			ZeroThreshold:  0.01,
			ZeroCount:      &prompb.Histogram_ZeroCountFloat{ZeroCountFloat: 3.5},
			PositiveSpans:  []prompb.BucketSpan{{Offset: 0, Length: 1}},
			PositiveCounts: []float64{7.5},
			Timestamp:      2000,
		}

		result := HistogramPromProtoToHistogramProto(promProto)

		assert.Equal(t, 20.5, result.GetCountFloat())
		assert.Equal(t, 3.5, result.GetZeroCountFloat())
		assert.True(t, result.IsFloatHistogram())
		assert.Equal(t, []float64{7.5}, result.PositiveCounts)
	})
}

func TestSpansConversions(t *testing.T) {
	t.Run("nil input", func(t *testing.T) {
		result := spansProtoToSpans(nil)
		assert.Empty(t, result)

		result2 := spansToSpansProto(nil)
		assert.Empty(t, result2)

		result3 := spansPromProtoToSpansProto(nil)
		assert.Empty(t, result3)
	})

	t.Run("round trip", func(t *testing.T) {
		original := []histogram.Span{
			{Offset: 0, Length: 2},
			{Offset: 3, Length: 5},
			{Offset: -1, Length: 1},
		}

		proto := spansToSpansProto(original)
		require.Len(t, proto, 3)
		assert.Equal(t, int32(0), proto[0].Offset)
		assert.Equal(t, uint32(2), proto[0].Length)

		result := spansProtoToSpans(proto)
		assert.Equal(t, original, result)
	})
}
