package cortexpb

import (
	"github.com/opentracing/opentracing-go"
)

// StreamWriteRequestCarrier is used to transfer trace
// information from/to a StreamWriteRequest.
type StreamWriteRequestCarrier StreamWriteRequest

func (c *StreamWriteRequestCarrier) Set(key, val string) {
	c.TraceContext = append(c.TraceContext, &Header{
		Key:    key,
		Values: []string{val},
	})
}

func (c *StreamWriteRequestCarrier) ForeachKey(handler func(key, val string) error) error {
	for _, h := range c.TraceContext {
		for _, v := range h.Values {
			if err := handler(h.Key, v); err != nil {
				return err
			}
		}
	}
	return nil
}

// InjectSpanIntoStreamWriteRequest makes req carry the trace context of span.
func InjectSpanIntoStreamWriteRequest(tracer opentracing.Tracer, span opentracing.Span, req *StreamWriteRequest) error {
	if tracer == nil || span == nil {
		return nil
	}

	return tracer.Inject(span.Context(), opentracing.HTTPHeaders, (*StreamWriteRequestCarrier)(req))
}

// GetParentSpanForStreamWriteRequest returns the span context req carries.
func GetParentSpanForStreamWriteRequest(tracer opentracing.Tracer, req *StreamWriteRequest) (opentracing.SpanContext, error) {
	if tracer == nil || len(req.TraceContext) == 0 {
		return nil, nil
	}

	extracted, err := tracer.Extract(opentracing.HTTPHeaders, (*StreamWriteRequestCarrier)(req))
	if err == opentracing.ErrSpanContextNotFound {
		err = nil
	}
	return extracted, err
}
