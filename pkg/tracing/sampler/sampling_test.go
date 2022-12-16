package sampler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/propagators/aws/xray"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func Test_ShouldSample(t *testing.T) {
	parentCtx := trace.ContextWithSpanContext(
		context.Background(),
		trace.NewSpanContext(trace.SpanContextConfig{
			TraceState: trace.TraceState{},
		}),
	)

	generator := xray.NewIDGenerator()

	tests := []struct {
		name     string
		fraction float64
	}{
		{
			name:     "should always sample",
			fraction: 1,
		},
		{
			name:     "should nerver sample",
			fraction: 0,
		},
		{
			name:     "should sample 50%",
			fraction: 0.5,
		},
		{
			name:     "should sample 10%",
			fraction: 0.1,
		},
	}

	totalIterations := 10000
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			totalSampled := 0
			s := NewXrayTraceIDRatioBased(tt.fraction)
			for i := 0; i < totalIterations; i++ {
				traceID, _ := generator.NewIDs(context.Background())
				r := s.ShouldSample(
					sdktrace.SamplingParameters{
						ParentContext: parentCtx,
						TraceID:       traceID,
						Name:          "test",
						Kind:          trace.SpanKindServer,
					})
				if r.Decision == sdktrace.RecordAndSample {
					totalSampled++
				}
			}

			tolerance := 0.1
			expected := tt.fraction * float64(totalIterations)
			require.InDelta(t, expected, totalSampled, expected*tolerance)
		})
	}
}
