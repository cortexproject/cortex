package zipkintracer

import "golang.org/x/net/trace"

// NetTraceIntegrator can be passed into a zipkintracer as NewSpanEventListener
// and causes all traces to be registered with the net/trace endpoint.
var NetTraceIntegrator = func() func(SpanEvent) {
	var tr trace.Trace
	return func(e SpanEvent) {
		switch t := e.(type) {
		case EventCreate:
			tr = trace.New("tracing", t.OperationName)
		case EventFinish:
			tr.Finish()
		case EventLog:
			if t.Payload != nil {
				tr.LazyPrintf("%s (payload %v)", t.Event, t.Payload)
			} else {
				tr.LazyPrintf("%s", t.Event)
			}
		}
	}
}
