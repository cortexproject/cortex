package cortexpbv2

import (
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPreallocTimeseriesV2SliceFromPool(t *testing.T) {
	t.Run("new instance is provided when not available to reuse", func(t *testing.T) {
		first := PreallocTimeseriesV2SliceFromPool()
		second := PreallocTimeseriesV2SliceFromPool()

		assert.NotSame(t, first, second)
	})

	t.Run("instance is cleaned before reusing", func(t *testing.T) {
		slice := PreallocTimeseriesV2SliceFromPool()
		slice = append(slice, PreallocTimeseriesV2{TimeSeries: &TimeSeries{}})
		ReuseSlice(slice)

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
		ts.Samples = []Sample{{Value: 1, Timestamp: 2}}
		ts.Exemplars = []Exemplar{{LabelsRefs: []uint32{1, 2}, Value: 1, Timestamp: 2}}
		ts.Histograms = []Histogram{{}}
		fmt.Println("ts.Histograms", len(ts.Histograms))
		ReuseTimeseries(ts)

		reused := TimeseriesV2FromPool()
		assert.Len(t, reused.LabelsRefs, 0)
		assert.Len(t, reused.Samples, 0)
		assert.Len(t, reused.Exemplars, 0)
		assert.Len(t, reused.Histograms, 0)
	})
}

func BenchmarkMarshallWriteRequest(b *testing.B) {
	ts := PreallocTimeseriesV2SliceFromPool()

	for i := 0; i < 100; i++ {
		ts = append(ts, PreallocTimeseriesV2{TimeSeries: TimeseriesV2FromPool()})
		ts[i].LabelsRefs = []uint32{1, 2, 3, 4, 5, 6, 7, 8}
		ts[i].Samples = []Sample{{Value: 1, Timestamp: 2}}
	}

	tests := []struct {
		name                string
		writeRequestFactory func() proto.Marshaler
		clean               func(in interface{})
	}{
		{
			name: "no-pool",
			writeRequestFactory: func() proto.Marshaler {
				return &WriteRequest{Timeseries: ts}
			},
			clean: func(in interface{}) {},
		},
		{
			name: "byte pool",
			writeRequestFactory: func() proto.Marshaler {
				w := &PreallocWriteRequestV2{}
				w.Timeseries = ts
				return w
			},
			clean: func(in interface{}) {
				ReuseWriteRequestV2(in.(*PreallocWriteRequestV2))
			},
		},
		{
			name: "byte and write pool",
			writeRequestFactory: func() proto.Marshaler {
				w := PreallocWriteRequestV2FromPool()
				w.Timeseries = ts
				return w
			},
			clean: func(in interface{}) {
				ReuseWriteRequestV2(in.(*PreallocWriteRequestV2))
			},
		},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				w := tc.writeRequestFactory()
				_, err := w.Marshal()
				require.NoError(b, err)
				tc.clean(w)
			}
			b.ReportAllocs()
		})
	}
}
