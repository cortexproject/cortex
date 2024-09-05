package tripperware

func (m *Sample) GetTimestampMs() int64 {
	if m != nil {
		if m.Sample != nil {
			return m.Sample.TimestampMs
		} else if m.Histogram != nil {
			return m.Histogram.TimestampMs
		}
	}
	return 0
}
