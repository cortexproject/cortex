package cortexpb

import (
	"sync"
)

var (
	expectedSymbols = 20

	slicePoolV2 = sync.Pool{
		New: func() interface{} {
			return make([]PreallocTimeseriesV2, 0, expectedTimeseries)
		},
	}

	timeSeriesPoolV2 = sync.Pool{
		New: func() interface{} {
			return &TimeSeriesV2{
				LabelsRefs: make([]uint32, 0, expectedLabels),
				Samples:    make([]Sample, 0, expectedSamplesPerSeries),
				Histograms: make([]Histogram, 0, expectedHistogramsPerSeries),
				Exemplars:  make([]ExemplarV2, 0, expectedExemplarsPerSeries),
				Metadata:   MetadataV2{},
			}
		},
	}

	writeRequestPoolV2 = sync.Pool{
		New: func() interface{} {
			return &PreallocWriteRequestV2{
				WriteRequestV2: WriteRequestV2{
					Symbols: make([]string, 0, expectedSymbols),
				},
			}
		},
	}
	bytePoolV2 = NewSlicePool(20)
)

// PreallocWriteRequestV2 is a WriteRequest which preallocs slices on Unmarshal.
type PreallocWriteRequestV2 struct {
	WriteRequestV2
	data *[]byte
}

// Unmarshal implements proto.Message.
func (p *PreallocWriteRequestV2) Unmarshal(dAtA []byte) error {
	p.Timeseries = PreallocTimeseriesV2SliceFromPool()
	return p.WriteRequestV2.Unmarshal(dAtA)
}

func (p *PreallocWriteRequestV2) Marshal() (dAtA []byte, err error) {
	size := p.Size()
	p.data = bytePool.GetSlice(size)
	dAtA = *p.data
	n, err := p.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

// PreallocTimeseriesV2 is a TimeSeries which preallocs slices on Unmarshal.
type PreallocTimeseriesV2 struct {
	*TimeSeriesV2
}

// Unmarshal implements proto.Message.
func (p *PreallocTimeseriesV2) Unmarshal(dAtA []byte) error {
	p.TimeSeriesV2 = TimeseriesV2FromPool()
	return p.TimeSeriesV2.Unmarshal(dAtA)
}

func ReuseWriteRequestV2(req *PreallocWriteRequestV2) {
	if req.data != nil {
		bytePoolV2.ReuseSlice(req.data)
		req.data = nil
	}
	req.Source = 0
	req.Symbols = nil
	req.Timeseries = nil
	writeRequestPoolV2.Put(req)
}

func PreallocWriteRequestV2FromPool() *PreallocWriteRequestV2 {
	return writeRequestPoolV2.Get().(*PreallocWriteRequestV2)
}

// PreallocTimeseriesV2SliceFromPool retrieves a slice of PreallocTimeseriesV2 from a sync.Pool.
// ReuseSlice should be called once done.
func PreallocTimeseriesV2SliceFromPool() []PreallocTimeseriesV2 {
	return slicePoolV2.Get().([]PreallocTimeseriesV2)
}

// ReuseSlice puts the slice back into a sync.Pool for reuse.
func ReuseSliceV2(ts []PreallocTimeseriesV2) {
	for i := range ts {
		ReuseTimeseriesV2(ts[i].TimeSeriesV2)
	}

	slicePoolV2.Put(ts[:0]) //nolint:staticcheck //see comment on slicePool for more details
}

// TimeseriesV2FromPool retrieves a pointer to a TimeSeries from a sync.Pool.
// ReuseTimeseries should be called once done, unless ReuseSlice was called on the slice that contains this TimeSeries.
func TimeseriesV2FromPool() *TimeSeriesV2 {
	return timeSeriesPoolV2.Get().(*TimeSeriesV2)
}

// ReuseTimeseries puts the timeseries back into a sync.Pool for reuse.
func ReuseTimeseriesV2(ts *TimeSeriesV2) {
	// clear ts lableRef and samples
	ts.LabelsRefs = ts.LabelsRefs[:0]
	ts.Samples = ts.Samples[:0]

	// clear exmplar labelrefs
	for i := range ts.Exemplars {
		ts.Exemplars[i].LabelsRefs = ts.Exemplars[i].LabelsRefs[:0]
	}

	for i := range ts.Histograms {
		ts.Histograms[i].Reset()
	}

	ts.Exemplars = ts.Exemplars[:0]
	ts.Histograms = ts.Histograms[:0]
	ts.Metadata = MetadataV2{}
	timeSeriesPoolV2.Put(ts)
}
