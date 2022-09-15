package sampler

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type mockGenerator struct {
	mockedValue float64
}

func (g *mockGenerator) Float64() float64 {
	return g.mockedValue
}

func Test_ShouldSample(t *testing.T) {
	traceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	parentCtx := trace.ContextWithSpanContext(
		context.Background(),
		trace.NewSpanContext(trace.SpanContextConfig{
			TraceState: trace.TraceState{},
		}),
	)

	tests := []struct {
		name             string
		samplingDecision sdktrace.SamplingDecision
		fraction         float64
		generator        randGenerator
	}{
		{
			name:             "should always sample",
			samplingDecision: sdktrace.RecordAndSample,
			fraction:         1,
			generator:        rand.New(rand.NewSource(rand.Int63())),
		},
		{
			name:             "should nerver sample",
			samplingDecision: sdktrace.Drop,
			fraction:         0,
			generator:        rand.New(rand.NewSource(rand.Int63())),
		},
		{
			name:             "should sample when fraction is above generated",
			samplingDecision: sdktrace.RecordAndSample,
			fraction:         0.5,
			generator:        &mockGenerator{0.2},
		},
		{
			name:             "should not sample when fraction is not above generated",
			samplingDecision: sdktrace.Drop,
			fraction:         0.5,
			generator:        &mockGenerator{0.8},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewRandomRatioBased(tt.fraction, tt.generator)
			for i := 0; i < 100; i++ {

				r := s.ShouldSample(
					sdktrace.SamplingParameters{
						ParentContext: parentCtx,
						TraceID:       traceID,
						Name:          "test",
						Kind:          trace.SpanKindServer,
					})

				require.Equal(t, tt.samplingDecision, r.Decision)
			}
		})
	}
}
