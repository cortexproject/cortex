package cortexpbv2

import (
	"sync"

	"github.com/cortexproject/cortex/pkg/cortexpb"
)

var (
	expectedTimeseries          = 100
	expectedLabels              = 20
	expectedSymbols             = 20
	expectedSamplesPerSeries    = 10
	expectedExemplarsPerSeries  = 1
	expectedHistogramsPerSeries = 1

	slicePool = sync.Pool{
		New: func() interface{} {
			return make([]PreallocTimeseriesV2, 0, expectedTimeseries)
		},
	}

	timeSeriesPool = sync.Pool{
		New: func() interface{} {
			return &TimeSeries{
				LabelsRefs: make([]uint32, 0, expectedLabels),
				Samples:    make([]Sample, 0, expectedSamplesPerSeries),
				Histograms: make([]Histogram, 0, expectedHistogramsPerSeries),
				Exemplars:  make([]Exemplar, 0, expectedExemplarsPerSeries),
				Metadata:   Metadata{},
			}
		},
	}

	writeRequestPool = sync.Pool{
		New: func() interface{} {
			return &PreallocWriteRequestV2{
				WriteRequest: WriteRequest{
					Symbols: make([]string, 0, expectedSymbols),
				},
			}
		},
	}
	bytePool = cortexpb.NewSlicePool(20)
)

// PreallocWriteRequestV2 is a WriteRequest which preallocs slices on Unmarshal.
type PreallocWriteRequestV2 struct {
	WriteRequest
	data *[]byte
}

// Unmarshal implements proto.Message.
func (p *PreallocWriteRequestV2) Unmarshal(dAtA []byte) error {
	p.Timeseries = PreallocTimeseriesV2SliceFromPool()
	return p.WriteRequest.Unmarshal(dAtA)
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
	*TimeSeries
}

// Unmarshal implements proto.Message.
func (p *PreallocTimeseriesV2) Unmarshal(dAtA []byte) error {
	p.TimeSeries = TimeseriesV2FromPool()
	return p.TimeSeries.Unmarshal(dAtA)
}

func ReuseWriteRequestV2(req *PreallocWriteRequestV2) {
	if req.data != nil {
		bytePool.ReuseSlice(req.data)
		req.data = nil
	}
	req.Source = 0
	req.Symbols = nil
	req.Timeseries = nil
	writeRequestPool.Put(req)
}

func PreallocWriteRequestV2FromPool() *PreallocWriteRequestV2 {
	return writeRequestPool.Get().(*PreallocWriteRequestV2)
}

// PreallocTimeseriesV2SliceFromPool retrieves a slice of PreallocTimeseriesV2 from a sync.Pool.
// ReuseSlice should be called once done.
func PreallocTimeseriesV2SliceFromPool() []PreallocTimeseriesV2 {
	return slicePool.Get().([]PreallocTimeseriesV2)
}

// ReuseSlice puts the slice back into a sync.Pool for reuse.
func ReuseSlice(ts []PreallocTimeseriesV2) {
	for i := range ts {
		ReuseTimeseries(ts[i].TimeSeries)
	}

	slicePool.Put(ts[:0]) //nolint:staticcheck //see comment on slicePool for more details
}

// TimeseriesV2FromPool retrieves a pointer to a TimeSeries from a sync.Pool.
// ReuseTimeseries should be called once done, unless ReuseSlice was called on the slice that contains this TimeSeries.
func TimeseriesV2FromPool() *TimeSeries {
	return timeSeriesPool.Get().(*TimeSeries)
}

// ReuseTimeseries puts the timeseries back into a sync.Pool for reuse.
func ReuseTimeseries(ts *TimeSeries) {
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
	ts.Metadata = Metadata{}
	timeSeriesPool.Put(ts)
}
