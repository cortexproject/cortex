package sampler

import (
	"fmt"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type randGenerator interface {
	Float64() float64
}

type RandomRatioBased struct {
	sdktrace.Sampler
	rnd      randGenerator
	fraction float64
}

// NewRandomRatioBased creates a sampler based on random number.
// fraction parameter should be between 0 and 1 where:
// fraction >= 1 it will always sample
// fraction <= 0 it will never sample
func NewRandomRatioBased(fraction float64, rnd randGenerator) sdktrace.Sampler {
	if fraction >= 1 {
		return sdktrace.AlwaysSample()
	} else if fraction <= 0 {
		return sdktrace.NeverSample()
	}

	return &RandomRatioBased{rnd: rnd, fraction: fraction}
}

func (s *RandomRatioBased) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	psc := trace.SpanContextFromContext(p.ParentContext)
	shouldSample := s.rnd.Float64() < s.fraction
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
	return fmt.Sprintf("RandomRatioBased{%g}", s.fraction)
}
