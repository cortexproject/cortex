package sampler

import (
	"encoding/binary"
	"fmt"
	"math"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type xrayTraceIDRatioBased struct {
	sdktrace.Sampler
	max uint64
}

// NewXrayTraceIDRatioBased creates a sampler based on random number.
// fraction parameter should be between 0 and 1 where:
// fraction >= 1 it will always sample
// fraction <= 0 it will never sample
func NewXrayTraceIDRatioBased(fraction float64) sdktrace.Sampler {
	if fraction >= 1 {
		return sdktrace.AlwaysSample()
	} else if fraction <= 0 {
		return sdktrace.NeverSample()
	}

	return &xrayTraceIDRatioBased{max: uint64(fraction * math.MaxUint64)}
}

func (s *xrayTraceIDRatioBased) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	// The default otel sampler pick the first 8 bytes to make the sampling decision and this is a problem to
	// xray case as the first 4 bytes on the xray traceId is the time of the original request and the random part are
	// the 12 last bytes, and so, this sampler pick the last 8 bytes to make the sampling decision.
	// Xray Trace format: https://docs.aws.amazon.com/xray/latest/devguide/xray-api-sendingdata.html
	// Xray Id Generator: https://github.com/open-telemetry/opentelemetry-go-contrib/blob/54f0bc5c0fd347cd6db9b7bc14c9f0c00dfcb36b/propagators/aws/xray/idgenerator.go#L58-L63
	// Ref: https://github.com/open-telemetry/opentelemetry-go/blob/7a60bc785d669fa6ad26ba70e88151d4df631d90/sdk/trace/sampling.go#L82-L95
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

func (s *xrayTraceIDRatioBased) Description() string {
	return fmt.Sprintf("xrayTraceIDRatioBased{%v}", s.max)
}
