package cortexpb

import (
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
