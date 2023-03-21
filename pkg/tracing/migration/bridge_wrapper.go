package migration

import (
	"github.com/opentracing/opentracing-go"
	bridge "go.opentelemetry.io/otel/bridge/opentracing"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type CortexBridgeTracerWrapper struct {
	bt *bridge.BridgeTracer
}

func NewCortexBridgeTracerWrapper(tracer trace.Tracer) (*CortexBridgeTracerWrapper, trace.TracerProvider) {
	bt, wp := bridge.NewTracerPair(tracer)
	return &CortexBridgeTracerWrapper{bt: bt}, wp
}

func (b *CortexBridgeTracerWrapper) StartSpan(operationName string, opts ...opentracing.StartSpanOption) opentracing.Span {
	return b.bt.StartSpan(operationName, opts...)
}

func (b *CortexBridgeTracerWrapper) Inject(sm opentracing.SpanContext, format interface{}, carrier interface{}) error {
	builtinFormat, ok := format.(opentracing.BuiltinFormat)

	if !ok {
		return opentracing.ErrUnsupportedFormat
	}

	switch builtinFormat {
	case opentracing.HTTPHeaders:
		if _, ok := carrier.(opentracing.HTTPHeadersCarrier); ok {
			return b.bt.Inject(sm, format, carrier)
		}

		// If the format is HTTPHeaders and the carrier is not HTTPHeadersCarrier the bridge returns an error:
		// see: https://github.com/open-telemetry/opentelemetry-go/blob/b9adb171b08e3375fb311520043c1aca611cbfec/bridge/opentracing/bridge.go#L661
		// and https://github.com/cortexproject/cortex/blob/c815b3cb61e4d0a3f01e9947d44fa111bc85aa08/pkg/frontend/v1/frontend.go#L158
		// in order to avoid the error we are bridging the carries via an HTTPHeadersCarrier
		// This code was inspired on https://github.com/thanos-io/thanos/blob/67880caabffac1f693954d8ce51715973a369e2b/pkg/tracing/migration/bridge.go
		otCarrier := opentracing.HTTPHeadersCarrier{}

		if err := b.bt.Inject(sm, format, otCarrier); err != nil {
			return err
		}

		if tw, ok := carrier.(opentracing.TextMapWriter); ok {
			return otCarrier.ForeachKey(func(key, val string) error {
				tw.Set(key, val)
				return nil
			})
		}

		return opentracing.ErrUnsupportedFormat
	default:
		return b.bt.Inject(sm, format, carrier)
	}
}

func (b *CortexBridgeTracerWrapper) Extract(format interface{}, carrier interface{}) (opentracing.SpanContext, error) {
	builtinFormat, ok := format.(opentracing.BuiltinFormat)

	if !ok {
		return nil, opentracing.ErrUnsupportedFormat
	}

	switch builtinFormat {
	case opentracing.HTTPHeaders:
		if _, ok := carrier.(opentracing.HTTPHeadersCarrier); ok {
			return b.bt.Extract(format, carrier)
		}

		// If the format is HTTPHeaders and the carrier is not HTTPHeadersCarrier the bridge returns an error:
		// see: https://github.com/open-telemetry/opentelemetry-go/blob/b9adb171b08e3375fb311520043c1aca611cbfec/bridge/opentracing/bridge.go#L702
		// and https://github.com/cortexproject/cortex/blob/c815b3cb61e4d0a3f01e9947d44fa111bc85aa08/pkg/frontend/v1/frontend.go#L158
		// in order to avoid the error we are bridging the carries via an HTTPHeadersCarrier
		// This code was inspired on https://github.com/thanos-io/thanos/blob/67880caabffac1f693954d8ce51715973a369e2b/pkg/tracing/migration/bridge.go
		if tr, ok := carrier.(opentracing.TextMapReader); ok {
			otCarrier := opentracing.HTTPHeadersCarrier{}
			err := tr.ForeachKey(func(key, val string) error {
				otCarrier.Set(key, val)
				return nil
			})
			if err != nil {
				return nil, err
			}

			return b.bt.Extract(format, otCarrier)
		}

		return nil, opentracing.ErrUnsupportedFormat
	default:
		return b.bt.Extract(format, carrier)
	}
}

func (b *CortexBridgeTracerWrapper) SetTextMapPropagator(propagator propagation.TextMapPropagator) {
	b.bt.SetTextMapPropagator(propagator)
}
