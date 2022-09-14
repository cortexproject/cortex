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
	rnd      randGenerator
	fraction float64
}

func NewRandomRatioBased(fraction float64, rnd randGenerator) sdktrace.Sampler {
	if fraction > 1 {
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
