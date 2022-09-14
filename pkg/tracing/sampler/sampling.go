package sampler

import (
	"fmt"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type randGenerator interface {
	Float64() float64
}

type RandTraceIDRatioBased struct {
	rnd      randGenerator
	fraction float64
}

func NewRandTraceIDRatioBased(fraction float64, rnd randGenerator) sdktrace.Sampler {
	if fraction > 1 {
		return sdktrace.AlwaysSample()
	} else if fraction <= 0 {
		return sdktrace.NeverSample()
	}

	return &RandTraceIDRatioBased{rnd: rnd, fraction: fraction}
}

func (s *RandTraceIDRatioBased) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
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

func (s *RandTraceIDRatioBased) Description() string {
	return fmt.Sprintf("RandTraceIDRatioBased{%g}", s.fraction)
}
