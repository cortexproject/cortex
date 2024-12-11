package cortexpb

import (
	"flag"
	"sync"
	"unsafe"
)

var (
	expectedTimeseries          = 100
	expectedLabels              = 20
	expectedSamplesPerSeries    = 10
	expectedExemplarsPerSeries  = 1
	expectedHistogramsPerSeries = 1

	/*
		We cannot pool these as pointer-to-slice because the place we use them is in WriteRequest which is generated from Protobuf
		and we don't have an option to make it a pointer. There is overhead here 24 bytes of garbage every time a PreallocTimeseries
		is re-used. But since the slices are far far larger, we come out ahead.
	*/
	slicePool = sync.Pool{
		New: func() interface{} {
			return make([]PreallocTimeseries, 0, expectedTimeseries)
		},
	}

	timeSeriesPool = sync.Pool{
		New: func() interface{} {
			return &TimeSeries{
				Labels:     make([]LabelPair, 0, expectedLabels),
				Samples:    make([]Sample, 0, expectedSamplesPerSeries),
				Exemplars:  make([]Exemplar, 0, expectedExemplarsPerSeries),
				Histograms: make([]Histogram, 0, expectedHistogramsPerSeries),
			}
		},
	}

	writeRequestPool = sync.Pool{
		New: func() interface{} {
			return &PreallocWriteRequest{
				WriteRequest: WriteRequest{},
			}
		},
	}
	bytePool = newSlicePool(20)
)

// PreallocConfig configures how structures will be preallocated to optimise
// proto unmarshalling.
type PreallocConfig struct{}

// RegisterFlags registers configuration settings.
func (PreallocConfig) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&expectedTimeseries, "ingester-client.expected-timeseries", expectedTimeseries, "Expected number of timeseries per request, used for preallocations.")
	f.IntVar(&expectedLabels, "ingester-client.expected-labels", expectedLabels, "Expected number of labels per timeseries, used for preallocations.")
	f.IntVar(&expectedSamplesPerSeries, "ingester-client.expected-samples-per-series", expectedSamplesPerSeries, "Expected number of samples per timeseries, used for preallocations.")
}

// PreallocWriteRequest is a WriteRequest which preallocs slices on Unmarshal.
type PreallocWriteRequest struct {
	WriteRequest
	data *[]byte
}

// Unmarshal implements proto.Message.
func (p *PreallocWriteRequest) Unmarshal(dAtA []byte) error {
	p.Timeseries = PreallocTimeseriesSliceFromPool()
	return p.WriteRequest.Unmarshal(dAtA)
}

// PreallocTimeseries is a TimeSeries which preallocs slices on Unmarshal.
type PreallocTimeseries struct {
	*TimeSeries
}

// Unmarshal implements proto.Message.
func (p *PreallocTimeseries) Unmarshal(dAtA []byte) error {
	p.TimeSeries = TimeseriesFromPool()
	return p.TimeSeries.Unmarshal(dAtA)
}

func (p *PreallocWriteRequest) Marshal() (dAtA []byte, err error) {
	size := p.Size()
	p.data = bytePool.getSlice(size)
	dAtA = *p.data
	n, err := p.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func ReuseWriteRequest(req *PreallocWriteRequest) {
	if req.data != nil {
		bytePool.reuseSlice(req.data)
		req.data = nil
	}
	req.Source = 0
	req.Metadata = nil
	req.Timeseries = nil
	writeRequestPool.Put(req)
}

func PreallocWriteRequestFromPool() *PreallocWriteRequest {
	return writeRequestPool.Get().(*PreallocWriteRequest)
}

func yoloString(buf []byte) string {
	return *((*string)(unsafe.Pointer(&buf)))
}

// PreallocTimeseriesSliceFromPool retrieves a slice of PreallocTimeseries from a sync.Pool.
// ReuseSlice should be called once done.
func PreallocTimeseriesSliceFromPool() []PreallocTimeseries {
	return slicePool.Get().([]PreallocTimeseries)
}

// ReuseSlice puts the slice back into a sync.Pool for reuse.
func ReuseSlice(ts []PreallocTimeseries) {
	for i := range ts {
		ReuseTimeseries(ts[i].TimeSeries)
	}

	slicePool.Put(ts[:0]) //nolint:staticcheck //see comment on slicePool for more details
}

// TimeseriesFromPool retrieves a pointer to a TimeSeries from a sync.Pool.
// ReuseTimeseries should be called once done, unless ReuseSlice was called on the slice that contains this TimeSeries.
func TimeseriesFromPool() *TimeSeries {
	return timeSeriesPool.Get().(*TimeSeries)
}

// ReuseTimeseries puts the timeseries back into a sync.Pool for reuse.
func ReuseTimeseries(ts *TimeSeries) {
	// Name and Value may point into a large gRPC buffer, so clear the reference to allow GC
	for i := 0; i < len(ts.Labels); i++ {
		ts.Labels[i].Name = ""
		ts.Labels[i].Value = ""
	}
	ts.Labels = ts.Labels[:0]
	ts.Samples = ts.Samples[:0]
	// Name and Value may point into a large gRPC buffer, so clear the reference in each exemplar to allow GC
	for i := range ts.Exemplars {
		for j := range ts.Exemplars[i].Labels {
			ts.Exemplars[i].Labels[j].Name = ""
			ts.Exemplars[i].Labels[j].Value = ""
		}
	}

	for i := range ts.Histograms {
		ts.Histograms[i].Reset()
	}

	ts.Exemplars = ts.Exemplars[:0]
	ts.Histograms = ts.Histograms[:0]
	timeSeriesPool.Put(ts)
}
