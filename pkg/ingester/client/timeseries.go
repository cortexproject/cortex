package client

import "flag"

var (
	expectedTimeseries       = flag.Int("ingester-client.expecter-timeseries", 100, "Expected number of timeseries per request, use for preallocations.")
	expectedLabels           = flag.Int("ingester-client.expected-labels", 20, "Expected number of labels per timeseries, used for preallocations.")
	expectedSamplesPerSeries = flag.Int("ingester-client.expected-samples-per-series", 10, "Expected number of samples per timeseries, used for preallocations.")
)

// PreallocWriteRequest is a WriteRequest which preallocs slices on Unmarshall.
type PreallocWriteRequest struct {
	WriteRequest
}

// Unmarshal implements proto.Message.
func (p *PreallocWriteRequest) Unmarshal(dAtA []byte) error {
	p.Timeseries = make([]PreallocTimeseries, 0, *expectedTimeseries)
	return p.WriteRequest.Unmarshal(dAtA)
}

// PreallocTimeseries is a TimeSeries which preallocs slices on Unmarshall.
type PreallocTimeseries struct {
	TimeSeries
}

// Unmarshal implements proto.Message.
func (p *PreallocTimeseries) Unmarshal(dAtA []byte) error {
	p.Labels = make([]LabelPair, 0, *expectedLabels)
	p.Samples = make([]Sample, 0, *expectedSamplesPerSeries)
	return p.TimeSeries.Unmarshal(dAtA)
}
