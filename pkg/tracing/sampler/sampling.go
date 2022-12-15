package sampler

import (
	"encoding/binary"
	"fmt"
	"math"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type RandomRatioBased struct {
	sdktrace.Sampler
	max uint64
}

// NewRandomRatioBased creates a sampler based on random number.
// fraction parameter should be between 0 and 1 where:
// fraction >= 1 it will always sample
// fraction <= 0 it will never sample
func NewRandomRatioBased(fraction float64) sdktrace.Sampler {
	if fraction >= 1 {
		return sdktrace.AlwaysSample()
	} else if fraction <= 0 {
		return sdktrace.NeverSample()
	}

	return &RandomRatioBased{max: uint64(fraction * math.MaxUint64)}
}

func (s *RandomRatioBased) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	val := binary.BigEndian.Uint64(p.TraceID[8:16])
	psc := trace.SpanContextFromContext(p.ParentContext)
	shouldSample := val < s.max
	if shouldSample {
		return sdktrace.SamplingResult{
			Decision:   sdktrace.RecordAndSample,
			Tracestate: psc.TraceState(),
		}
	}
	return sdktrace.SamplingResult{
		Decision:   sdktrace.Drop,
		Tracestate: psc.TraceState(),
	}
}

func (s *RandomRatioBased) Description() string {
	return fmt.Sprintf("RandomRatioBased{%v}", s.max)
}
