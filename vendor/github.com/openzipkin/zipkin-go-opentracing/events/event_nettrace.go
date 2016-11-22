package events

import (
	"github.com/openzipkin/zipkin-go-opentracing"
	"golang.org/x/net/trace"
)

// NetTraceIntegrator can be passed into a zipkintracer as NewSpanEventListener
// and causes all traces to be registered with the net/trace endpoint.
var NetTraceIntegrator = func() func(zipkintracer.SpanEvent) {
	var tr trace.Trace
	return func(e zipkintracer.SpanEvent) {
		switch t := e.(type) {
		case zipkintracer.EventCreate:
			tr = trace.New("tracing", t.OperationName)
		case zipkintracer.EventFinish:
			tr.Finish()
		case zipkintracer.EventLog:
			if t.Payload != nil {
				tr.LazyPrintf("%s (payload %v)", t.Event, t.Payload)
			} else {
				tr.LazyPrintf("%s", t.Event)
			}
		}
	}
}
