package migration

import (
	"context"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/cortexproject/cortex/pkg/util/httpgrpcutil"
)

var (
	traceID    trace.TraceID = [16]byte{byte(10)}
	spanID     trace.SpanID  = [8]byte{byte(11)}
	traceIDKey               = "Traceid"
	spanIDKey                = "Spanid"
	noopTracer               = noop.NewTracerProvider().Tracer("")
	noopSpan                 = func() trace.Span {
		_, s := noopTracer.Start(context.Background(), "")
		return s
	}()
)

type mockSpan struct {
	trace.Span
}

func (s *mockSpan) SpanContext() trace.SpanContext {
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID,
		SpanID:  spanID,
	})
}

type mockTracer struct {
	trace.Tracer
}

func (mt *mockTracer) Start(ctx context.Context, _ string, _ ...trace.SpanStartOption) (context.Context, trace.Span) {
	return ctx, &mockSpan{noopSpan}
}

type mockPropagator struct {
	propagation.TextMapPropagator
	extracted map[string]string
}

func (p *mockPropagator) Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	sc := trace.SpanFromContext(ctx).SpanContext()
	carrier.Set(traceIDKey, sc.TraceID().String())
	carrier.Set(spanIDKey, sc.SpanID().String())
}

func (p *mockPropagator) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	tID, _ := trace.TraceIDFromHex(carrier.Get(traceIDKey))
	sID, _ := trace.SpanIDFromHex(carrier.Get(spanIDKey))
	scc := trace.SpanContextConfig{
		TraceID: tID,
		SpanID:  sID,
	}

	p.extracted = map[string]string{}
	for _, k := range carrier.Keys() {
		p.extracted[k] = carrier.Get(k)
	}

	return trace.ContextWithRemoteSpanContext(ctx, trace.NewSpanContext(scc))
}

func TestCortexBridgeTracerWrapper_Inject(t *testing.T) {
	tests := []struct {
		name         string
		format       interface{}
		carrier      interface{}
		wantedValues map[string]string
	}{
		{
			name:   "test HTTPHeadersCarrier",
			format: opentracing.HTTPHeaders,
			carrier: opentracing.HTTPHeadersCarrier{
				"Key": []string{"value"},
			},
			wantedValues: map[string]string{
				"Key":      "value",
				traceIDKey: traceID.String(),
			},
		},
		{
			name:   "test HttpgrpcHeadersCarrier",
			format: opentracing.HTTPHeaders,
			carrier: &httpgrpcutil.HttpgrpcHeadersCarrier{
				Headers: []*httpgrpc.Header{
					{
						Key:    "Key",
						Values: []string{"value"},
					},
				},
			},
			wantedValues: map[string]string{
				"Key":      "value",
				traceIDKey: traceID.String(),
			},
		},
		{
			name:   "test HeaderCarrier",
			format: opentracing.TextMap,
			carrier: &propagation.HeaderCarrier{
				"Key": []string{"value"},
			},
			wantedValues: map[string]string{
				"Key":      "value",
				traceIDKey: traceID.String(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &mockPropagator{}
			injected := map[string]string{}
			bw, _ := NewCortexBridgeTracerWrapper(&mockTracer{})
			bw.SetTextMapPropagator(p)
			sc := bw.StartSpan("test")

			err := bw.Inject(sc.Context(), tt.format, tt.carrier)
			require.NoError(t, err)

			if tr, ok := tt.carrier.(opentracing.TextMapReader); ok {
				err := tr.ForeachKey(func(key, val string) error {
					injected[key] = val
					return nil
				})
				require.NoError(t, err)
			}

			if tmc, ok := tt.carrier.(propagation.TextMapCarrier); ok {
				for _, k := range tmc.Keys() {
					injected[k] = tmc.Get(k)
				}
			}

			_, err = bw.Extract(tt.format, tt.carrier)

			for k, v := range tt.wantedValues {
				require.Equal(t, injected[k], v)
				require.Equal(t, p.extracted[k], v)
			}
			require.NoError(t, err)
		})
	}
}
