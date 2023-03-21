package cortexpb

import (
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLabelAdapter_Marshal(t *testing.T) {
	tests := []struct {
		bs *LabelAdapter
	}{
		{&LabelAdapter{Name: "foo", Value: "bar"}},
		{&LabelAdapter{Name: "very long label name", Value: "very long label value"}},
		{&LabelAdapter{Name: "", Value: "foo"}},
		{&LabelAdapter{}},
	}
	for _, tt := range tests {
		t.Run(tt.bs.Name, func(t *testing.T) {
			bytes, err := tt.bs.Marshal()
			require.NoError(t, err)
			lbs := &LabelAdapter{}
			require.NoError(t, lbs.Unmarshal(bytes))
			require.EqualValues(t, tt.bs, lbs)
		})
	}
}

func TestPreallocTimeseriesSliceFromPool(t *testing.T) {
	t.Run("new instance is provided when not available to reuse", func(t *testing.T) {
		first := PreallocTimeseriesSliceFromPool()
		second := PreallocTimeseriesSliceFromPool()

		assert.NotSame(t, first, second)
	})

	t.Run("instance is cleaned before reusing", func(t *testing.T) {
		slice := PreallocTimeseriesSliceFromPool()
		slice = append(slice, PreallocTimeseries{TimeSeries: &TimeSeries{}})
		ReuseSlice(slice)

		reused := PreallocTimeseriesSliceFromPool()
		assert.Len(t, reused, 0)
	})
}

func TestTimeseriesFromPool(t *testing.T) {
	t.Run("new instance is provided when not available to reuse", func(t *testing.T) {
		first := TimeseriesFromPool()
		second := TimeseriesFromPool()

		assert.NotSame(t, first, second)
	})

	t.Run("instance is cleaned before reusing", func(t *testing.T) {
		ts := TimeseriesFromPool()
		ts.Labels = []LabelAdapter{{Name: "foo", Value: "bar"}}
		ts.Samples = []Sample{{Value: 1, TimestampMs: 2}}
		ReuseTimeseries(ts)

		reused := TimeseriesFromPool()
		assert.Len(t, reused.Labels, 0)
		assert.Len(t, reused.Samples, 0)
	})
}

func BenchmarkMarshallWriteRequest(b *testing.B) {
	ts := PreallocTimeseriesSliceFromPool()

	for i := 0; i < 100; i++ {
		ts = append(ts, PreallocTimeseries{TimeSeries: TimeseriesFromPool()})
		ts[i].Labels = []LabelAdapter{
			{Name: "foo", Value: "bar"},
			{Name: "very long label name", Value: "very long label value"},
			{Name: "very long label name 2", Value: "very long label value 2"},
			{Name: "very long label name 3", Value: "very long label value 3"},
			{Name: "int", Value: fmt.Sprint(i)},
		}
		ts[i].Samples = []Sample{{Value: 1, TimestampMs: 2}}
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
				w := &PreallocWriteRequest{}
				w.Timeseries = ts
				return w
			},
			clean: func(in interface{}) {
				ReuseWriteRequest(in.(*PreallocWriteRequest))
			},
		},
		{
			name: "byte and write pool",
			writeRequestFactory: func() proto.Marshaler {
				w := PreallocWriteRequestFromPool()
				w.Timeseries = ts
				return w
			},
			clean: func(in interface{}) {
				ReuseWriteRequest(in.(*PreallocWriteRequest))
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
