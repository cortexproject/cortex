package cortexpb

import (
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/model/labels"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPreallocTimeseriesV2SliceFromPool(t *testing.T) {
	t.Run("new instance is provided when not available to reuse", func(t *testing.T) {
		first := PreallocTimeseriesV2SliceFromPool()
		second := PreallocTimeseriesV2SliceFromPool()

		assert.NotSame(t, &first, &second)
	})

	t.Run("instance is cleaned before reusing", func(t *testing.T) {
		slice := PreallocTimeseriesV2SliceFromPool()
		slice = append(slice, PreallocTimeseriesV2{TimeSeriesV2: &TimeSeriesV2{}})
		ReuseSliceV2(slice)

		reused := PreallocTimeseriesV2SliceFromPool()
		assert.Len(t, reused, 0)
	})
}

func TestTimeseriesV2FromPool(t *testing.T) {
	t.Run("new instance is provided when not available to reuse", func(t *testing.T) {
		first := TimeseriesV2FromPool()
		second := TimeseriesV2FromPool()

		assert.NotSame(t, first, second)
	})

	t.Run("instance is cleaned before reusing", func(t *testing.T) {
		ts := TimeseriesV2FromPool()
		ts.LabelsRefs = []uint32{1, 2}
		ts.Samples = []Sample{{Value: 1, TimestampMs: 2}}
		ts.Exemplars = []ExemplarV2{{LabelsRefs: []uint32{1, 2}, Value: 1, Timestamp: 2}}
		ts.Histograms = []Histogram{{}}
		ReuseTimeseriesV2(ts)

		reused := TimeseriesV2FromPool()
		assert.Len(t, reused.LabelsRefs, 0)
		assert.Len(t, reused.Samples, 0)
		assert.Len(t, reused.Exemplars, 0)
		assert.Len(t, reused.Histograms, 0)
	})
}

func TestReuseWriteRequestV2(t *testing.T) {
	req := PreallocWriteRequestV2FromPool()

	// Populate req with some data.
	req.Source = RULE
	req.Symbols = append(req.Symbols, "", "__name__", "test")

	tsSlice := PreallocTimeseriesV2SliceFromPool()
	tsSlice = append(tsSlice, PreallocTimeseriesV2{TimeSeriesV2: TimeseriesV2FromPool()})
	req.Timeseries = tsSlice

	// Capture backing array before reuse
	symbolsBackingArray := req.Symbols[:cap(req.Symbols)]
	require.Equal(t, "__name__", symbolsBackingArray[1])
	require.Equal(t, "test", symbolsBackingArray[2])

	// Put the request back into the pool
	ReuseWriteRequestV2(req)

	// Verify clearing directly on the backing array
	for i, s := range symbolsBackingArray[:3] {
		assert.Equalf(t, "", s, "symbol at index %d not cleared", i)
	}

	// Source is reset to default
	assert.Equal(t, API, req.Source)
	// The symbol length is properly reset to 0.
	assert.Len(t, req.Symbols, 0)
	// Timeseries slice is nil
	assert.Nil(t, req.Timeseries)
}

func TestPreallocWriteRequestV2Reset(t *testing.T) {
	t.Run("preserves Symbols capacity", func(t *testing.T) {
		const symbolsCap = 100
		req := &PreallocWriteRequestV2{
			WriteRequestV2: WriteRequestV2{
				Symbols: make([]string, 0, symbolsCap),
			},
		}
		req.Symbols = append(req.Symbols, "a", "b", "c")

		ptrBefore := &req.Symbols[:cap(req.Symbols)][0]

		req.Reset()

		assert.Equal(t, 0, len(req.Symbols), "Symbols length should be 0 after Reset")
		assert.Equal(t, symbolsCap, cap(req.Symbols), "Symbols capacity should be preserved after Reset")
		assert.Same(t, ptrBefore, &req.Symbols[:cap(req.Symbols)][0], "Symbols backing array should be reused after Reset")
	})

	t.Run("clears non-Symbols WriteRequestV2 fields", func(t *testing.T) {
		b := []byte{1, 2, 3}
		req := &PreallocWriteRequestV2{
			WriteRequestV2: WriteRequestV2{
				Source:                  RULE,
				SkipLabelNameValidation: true,
				Timeseries:              []PreallocTimeseriesV2{{TimeSeriesV2: &TimeSeriesV2{}}},
			},
			data: &b,
		}

		req.Reset()

		assert.Equal(t, SourceEnum(0), req.Source)
		assert.False(t, req.SkipLabelNameValidation)
		assert.Nil(t, req.Timeseries)
		assert.Nil(t, req.data)
	})

	t.Run("Unmarshal after Reset reuses Symbols backing array", func(t *testing.T) {
		const symbolsCount = 50
		symbols := make([]string, symbolsCount)
		for i := range symbols {
			symbols[i] = fmt.Sprintf("symbol_%04d", i)
		}
		data, err := (&WriteRequestV2{Symbols: symbols}).Marshal()
		require.NoError(t, err)

		req := &PreallocWriteRequestV2{
			WriteRequestV2: WriteRequestV2{
				Symbols: make([]string, 0, symbolsCount*2),
			},
		}

		// Simulate Reset in util.ParseProtoReader()
		req.Reset()
		ptrAfterReset := &req.Symbols[:cap(req.Symbols)][0]
		capAfterReset := cap(req.Symbols)

		require.NoError(t, req.WriteRequestV2.Unmarshal(data))
		assert.Equal(t, symbolsCount, len(req.Symbols))
		assert.Equal(t, capAfterReset, cap(req.Symbols), "capacity should not change: Unmarshal reused the existing backing array")
		assert.Same(t, ptrAfterReset, &req.Symbols[:cap(req.Symbols)][0], "backing array pointer should be identical: no new allocation occurred")
	})
}

func BenchmarkMarshallWriteRequestV2(b *testing.B) {
	ts := PreallocTimeseriesV2SliceFromPool()

	numOfSeries := 100
	st := writev2.NewSymbolTable()
	lbs := labels.FromStrings(labels.MetricName, "foo", "labelName1", "labelValue1", "labelName2", "labelValue2", "labelName3", "labelValue3")
	st.SymbolizeLabels(lbs, nil)
	symbols := st.Symbols()
	for i := range numOfSeries {
		ts = append(ts, PreallocTimeseriesV2{TimeSeriesV2: TimeseriesV2FromPool()})
		ts[i].LabelsRefs = []uint32{1, 2, 3, 4, 5, 6, 7, 8}
		ts[i].Samples = []Sample{{Value: 1, TimestampMs: 2}}
	}

	tests := []struct {
		name                string
		writeRequestFactory func() proto.Marshaler
		clean               func(in any)
	}{
		{
			name: "no-pool",
			writeRequestFactory: func() proto.Marshaler {
				return &WriteRequestV2{Symbols: symbols, Timeseries: ts}
			},
			clean: func(in any) {},
		},
		{
			name: "byte pool",
			writeRequestFactory: func() proto.Marshaler {
				w := &PreallocWriteRequestV2{}
				w.Timeseries = ts
				w.Symbols = symbols
				return w
			},
			clean: func(in any) {
				ReuseWriteRequestV2(in.(*PreallocWriteRequestV2))
			},
		},
		{
			name: "byte and write pool",
			writeRequestFactory: func() proto.Marshaler {
				w := PreallocWriteRequestV2FromPool()
				w.Timeseries = ts
				w.Symbols = symbols
				return w
			},
			clean: func(in any) {
				ReuseWriteRequestV2(in.(*PreallocWriteRequestV2))
			},
		},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			for b.Loop() {
				w := tc.writeRequestFactory()
				_, err := w.Marshal()
				require.NoError(b, err)
				tc.clean(w)
			}
			b.ReportAllocs()
		})
	}
}
