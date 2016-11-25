package otgrpc

import "github.com/opentracing/opentracing-go"

// Option instances may be used in OpenTracing(Server|Client)Interceptor
// initialization.
//
// See this post about the "functional options" pattern:
// http://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis
type Option func(o *options)

// LogPayloads returns an Option that tells the OpenTracing instrumentation to
// try to log application payloads in both directions.
func LogPayloads() Option {
	return func(o *options) {
		o.logPayloads = true
	}
}

// SpanDecoratorFunc provides an (optional) mechanism for otgrpc users to add
// arbitrary tags/logs/etc to the opentracing.Span associated with client
// and/or server RPCs.
type SpanDecoratorFunc func(
	span opentracing.Span,
	method string,
	req, resp interface{},
	grpcError error)

// SpanDecorator binds a function that decorates gRPC Spans.
func SpanDecorator(decorator SpanDecoratorFunc) Option {
	return func(o *options) {
		o.decorator = decorator
	}
}

// The internal-only options struct. Obviously overkill at the moment; but will
// scale well as production use dictates other configuration and tuning
// parameters.
type options struct {
	logPayloads bool
	decorator   SpanDecoratorFunc
}

// newOptions returns the default options.
func newOptions() *options {
	return &options{
		logPayloads: false,
	}
}

func (o *options) apply(opts ...Option) {
	for _, opt := range opts {
		opt(o)
	}
}
