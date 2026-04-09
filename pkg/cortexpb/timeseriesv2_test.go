package cortexpb

import (
	"fmt"
	"runtime"
	"sync"
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
		ts.CreatedTimestamp = 12345
		ts.Metadata = MetadataV2{Type: 1, HelpRef: 2, UnitRef: 3}
		ReuseTimeseriesV2(ts)

		reused := TimeseriesV2FromPool()
		assert.Len(t, reused.LabelsRefs, 0)
		assert.Len(t, reused.Samples, 0)
		assert.Len(t, reused.Exemplars, 0)
		assert.Len(t, reused.Histograms, 0)
		assert.Zero(t, reused.CreatedTimestamp)
		assert.Zero(t, reused.Metadata.Type)
		assert.Zero(t, reused.Metadata.HelpRef)
		assert.Zero(t, reused.Metadata.UnitRef)
	})
}

func TestReuseWriteRequestV2(t *testing.T) {
	t.Run("resets fields to default and cleans backing array", func(t *testing.T) {
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
	})
	t.Run("updates dynamic capacity", func(t *testing.T) {
		currentCap := dynamicSymbolsCapacity.Load()
		newCap := int(currentCap) + 100 // Increase capacity

		req := PreallocWriteRequestV2FromPool()
		req.Symbols = make([]string, newCap)
		req.Timeseries = PreallocTimeseriesV2SliceFromPool()

		ReuseWriteRequestV2(req)

		// Verify that the dynamic capacity has been updated
		expectedCap := max((currentCap*9+int64(newCap))/10, int64(initialSymbolsCapacity))
		assert.Equal(t, expectedCap, dynamicSymbolsCapacity.Load())
	})
	t.Run("outlier capacity does not update dynamic capacity and is discarded", func(t *testing.T) {
		currentCap := dynamicSymbolsCapacity.Load()
		outlierCap := int(maxSymbolsCapacity) + 100 // Exceeds the max limit

		req := PreallocWriteRequestV2FromPool()
		req.Symbols = make([]string, outlierCap)
		req.Timeseries = PreallocTimeseriesV2SliceFromPool()

		ReuseWriteRequestV2(req)

		// Verify dynamic capacity didn't increase due to out-of-bound outlier
		assert.Equal(t, currentCap, dynamicSymbolsCapacity.Load())
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

func BenchmarkWriteRequestV2Pool_CompareFixedSymbolCapaWithDynamic(b *testing.B) {
	const (
		numVariants = 10
		gcInterval  = 100
	)

	scenarios := []struct {
		name                 string
		symbolMin, symbolMax int
	}{
		{"symbol_range_200_2000", 200, 2000},
		{"symbol_range_1000_6000", 1000, 6000},
		{"symbol_range_1000_20000 (exceed maxSymbolsCapacity)", 1000, 20000},
	}

	for _, sc := range scenarios {
		variants := make([][]string, numVariants)
		for i := range numVariants {
			size := sc.symbolMin + (sc.symbolMax-sc.symbolMin)*i/(numVariants-1)
			syms := make([]string, size)
			for j := range syms {
				syms[j] = fmt.Sprintf("label_%04d", j)
			}
			variants[i] = syms
		}

		b.Run(sc.name, func(b *testing.B) {
			b.Run("dynamic_capacity", func(b *testing.B) {
				saved := dynamicSymbolsCapacity.Load()
				dynamicSymbolsCapacity.Store(int64(initialSymbolsCapacity))
				defer dynamicSymbolsCapacity.Store(saved)

				// Converge EMA to ensure dynamic capacity is adjusted to the range of variants.
				for i := range 100 * numVariants {
					req := PreallocWriteRequestV2FromPool()
					req.Symbols = append(req.Symbols, variants[i%numVariants]...)
					ReuseWriteRequestV2(req)
				}

				b.ReportAllocs()
				var idx int
				for b.Loop() {
					if idx > 0 && idx%gcInterval == 0 {
						runtime.GC() // Periodically run GC to simulate real-world conditions where the pool may be emptied.
					}
					req := PreallocWriteRequestV2FromPool()
					req.Symbols = append(req.Symbols, variants[idx%numVariants]...)
					idx++
					ReuseWriteRequestV2(req)
				}
			})

			b.Run("fixed_capacity", func(b *testing.B) {
				fixedPool := sync.Pool{
					New: func() any {
						return &PreallocWriteRequestV2{
							WriteRequestV2: WriteRequestV2{
								Symbols: make([]string, 0, initialSymbolsCapacity),
							},
						}
					},
				}

				b.ReportAllocs()
				var idx int
				for b.Loop() {
					if idx > 0 && idx%gcInterval == 0 {
						runtime.GC()
					}
					req := fixedPool.Get().(*PreallocWriteRequestV2)
					req.Symbols = append(req.Symbols, variants[idx%numVariants]...)
					idx++

					// Same cleanup as ReuseWriteRequestV2, minus the EMA update.
					req.Source = 0
					symbolsCap := int64(cap(req.Symbols))
					if symbolsCap > maxSymbolsCapacity {
						if req.Timeseries != nil {
							ReuseSliceV2(req.Timeseries)
							req.Timeseries = nil
						}
						continue
					}
					for i := range req.Symbols {
						req.Symbols[i] = ""
					}
					req.Symbols = req.Symbols[:0]
					if req.Timeseries != nil {
						ReuseSliceV2(req.Timeseries)
						req.Timeseries = nil
					}
					fixedPool.Put(req)
				}
			})

			b.Run("fixed_capacity_2048", func(b *testing.B) {
				fixedPool2048 := sync.Pool{
					New: func() any {
						return &PreallocWriteRequestV2{
							WriteRequestV2: WriteRequestV2{
								Symbols: make([]string, 0, 2048),
							},
						}
					},
				}

				b.ReportAllocs()
				var idx int
				for b.Loop() {
					if idx > 0 && idx%gcInterval == 0 {
						runtime.GC()
					}
					req := fixedPool2048.Get().(*PreallocWriteRequestV2)
					req.Symbols = append(req.Symbols, variants[idx%numVariants]...)
					idx++

					// Same cleanup as ReuseWriteRequestV2, minus the EMA update.
					req.Source = 0
					symbolsCap := int64(cap(req.Symbols))
					if symbolsCap > maxSymbolsCapacity {
						if req.Timeseries != nil {
							ReuseSliceV2(req.Timeseries)
							req.Timeseries = nil
						}
						continue
					}
					for i := range req.Symbols {
						req.Symbols[i] = ""
					}
					req.Symbols = req.Symbols[:0]
					if req.Timeseries != nil {
						ReuseSliceV2(req.Timeseries)
						req.Timeseries = nil
					}
					fixedPool2048.Put(req)
				}
			})
		})
	}
}
