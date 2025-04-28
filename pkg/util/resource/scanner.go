package resource

import (
	"fmt"
	"runtime/metrics"
)

const (
	heapMetricName = "/memory/classes/heap/objects:bytes"
)

type scanner interface {
	scan() (float64, error)
}

type noopScanner struct{}

func (s *noopScanner) scan() (float64, error) {
	return 0, nil
}

type heapScanner struct {
	metricSamples []metrics.Sample
}

func newHeapScanner() (scanner, error) {
	metricSamples := make([]metrics.Sample, 1)
	metricSamples[0].Name = heapMetricName
	metrics.Read(metricSamples)

	for _, sample := range metricSamples {
		if sample.Value.Kind() == metrics.KindBad {
			return nil, fmt.Errorf("metric %s is not supported", sample.Name)
		}
	}

	return &heapScanner{metricSamples: metricSamples}, nil
}

func (s *heapScanner) scan() (float64, error) {
	metrics.Read(s.metricSamples)
	return float64(s.metricSamples[0].Value.Uint64()), nil
}
