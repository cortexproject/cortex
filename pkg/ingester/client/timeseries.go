package client

// PreallocTimeseries is a TimeSeries which preallocs slices on Unmarshall.
type PreallocTimeseries struct {
	TimeSeries
}

// Unmarshal implements proto.Message.
func (p *PreallocTimeseries) Unmarshal(dAtA []byte) error {
	p.Labels = make([]LabelPair, 0, 20)
	p.Samples = make([]Sample, 0, 10)
	return p.TimeSeries.Unmarshal(dAtA)
}
