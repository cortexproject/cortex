package zipkintracer

import (
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"strconv"
	"time"

	otext "github.com/opentracing/opentracing-go/ext"

	"github.com/openzipkin/zipkin-go-opentracing/_thrift/gen-go/zipkincore"
	"github.com/openzipkin/zipkin-go-opentracing/flag"
)

var (
	// SpanKindResource will be regarded as a SA annotation by Zipkin.
	SpanKindResource = otext.SpanKindEnum("resource")
)

// Recorder implements the SpanRecorder interface.
type Recorder struct {
	collector Collector
	debug     bool
	endpoint  *zipkincore.Endpoint
}

// NewRecorder creates a new Zipkin Recorder backed by the provided Collector.
func NewRecorder(c Collector, debug bool, hostPort, serviceName string) SpanRecorder {
	return &Recorder{
		collector: c,
		debug:     debug,
		endpoint:  makeEndpoint(hostPort, serviceName),
	}
}

// RecordSpan converts a RawSpan into the Zipkin representation of a span
// and records it to the underlying collector.
func (r *Recorder) RecordSpan(sp RawSpan) {
	if !sp.Context.Sampled {
		return
	}

	var parentSpanID *int64
	if sp.Context.ParentSpanID != nil {
		id := int64(*sp.Context.ParentSpanID)
		parentSpanID = &id
	}

	var traceIDHigh *int64
	if sp.Context.TraceID.High > 0 {
		tidh := int64(sp.Context.TraceID.High)
		traceIDHigh = &tidh
	}

	span := &zipkincore.Span{
		Name:        sp.Operation,
		ID:          int64(sp.Context.SpanID),
		TraceID:     int64(sp.Context.TraceID.Low),
		TraceIDHigh: traceIDHigh,
		ParentID:    parentSpanID,
		Debug:       r.debug || (sp.Context.Flags&flag.Debug == flag.Debug),
	}
	// only send timestamp and duration if this process owns the current span.
	if sp.Context.Owner {
		timestamp := sp.Start.UnixNano() / 1e3
		duration := sp.Duration.Nanoseconds() / 1e3
		// since we always time our spans we will round up to 1 microsecond if the
		// span took less.
		if duration == 0 {
			duration = 1
		}
		span.Timestamp = &timestamp
		span.Duration = &duration
	}
	if kind, ok := sp.Tags[string(otext.SpanKind)]; ok {
		switch kind {
		case otext.SpanKindRPCClient, otext.SpanKindRPCClientEnum:
			annotate(span, sp.Start, zipkincore.CLIENT_SEND, r.endpoint)
			annotate(span, sp.Start.Add(sp.Duration), zipkincore.CLIENT_RECV, r.endpoint)
		case otext.SpanKindRPCServer, otext.SpanKindRPCServerEnum:
			annotate(span, sp.Start, zipkincore.SERVER_RECV, r.endpoint)
			annotate(span, sp.Start.Add(sp.Duration), zipkincore.SERVER_SEND, r.endpoint)
		case SpanKindResource:
			serviceName, ok := sp.Tags[string(otext.PeerService)]
			if !ok {
				serviceName = r.endpoint.GetServiceName()
			}
			host, ok := sp.Tags[string(otext.PeerHostname)].(string)
			if !ok {
				if r.endpoint.GetIpv4() > 0 {
					ip := make([]byte, 4)
					binary.BigEndian.PutUint32(ip, uint32(r.endpoint.GetIpv4()))
					host = net.IP(ip).To4().String()
				} else {
					ip := r.endpoint.GetIpv6()
					host = net.IP(ip).String()
				}
			}
			var sPort string
			port, ok := sp.Tags[string(otext.PeerPort)]
			if !ok {
				sPort = strconv.FormatInt(int64(r.endpoint.GetPort()), 10)
			} else {
				sPort = strconv.FormatInt(int64(port.(uint16)), 10)
			}
			re := makeEndpoint(net.JoinHostPort(host, sPort), serviceName.(string))
			if re != nil {
				annotateBinary(span, zipkincore.SERVER_ADDR, serviceName, re)
			} else {
				fmt.Printf("endpoint creation failed: host: %q port: %q", host, sPort)
			}
			annotate(span, sp.Start, zipkincore.CLIENT_SEND, r.endpoint)
			annotate(span, sp.Start.Add(sp.Duration), zipkincore.CLIENT_RECV, r.endpoint)
		default:
			annotateBinary(span, zipkincore.LOCAL_COMPONENT, r.endpoint.GetServiceName(), r.endpoint)
		}
	} else {
		annotateBinary(span, zipkincore.LOCAL_COMPONENT, r.endpoint.GetServiceName(), r.endpoint)
	}

	for key, value := range sp.Tags {
		annotateBinary(span, key, value, r.endpoint)
	}

	for _, spLog := range sp.Logs {
		if logs, err := MaterializeWithJSON(spLog.Fields); err != nil {
			fmt.Printf("JSON serialization of OpenTracing LogFields failed: %+v", err)
		} else {
			annotate(span, spLog.Timestamp, string(logs), r.endpoint)
		}
	}
	_ = r.collector.Collect(span)
}

// annotate annotates the span with the given value.
func annotate(span *zipkincore.Span, timestamp time.Time, value string, host *zipkincore.Endpoint) {
	if timestamp.IsZero() {
		timestamp = time.Now()
	}
	span.Annotations = append(span.Annotations, &zipkincore.Annotation{
		Timestamp: timestamp.UnixNano() / 1e3,
		Value:     value,
		Host:      host,
	})
}

// annotateBinary annotates the span with a key and a value that will be []byte
// encoded.
func annotateBinary(span *zipkincore.Span, key string, value interface{}, host *zipkincore.Endpoint) {
	var a zipkincore.AnnotationType
	var b []byte
	// We are not using zipkincore.AnnotationType_I16 for types that could fit
	// as reporting on it seems to be broken on the zipkin web interface
	// (however, we can properly extract the number from zipkin storage
	// directly). int64 has issues with negative numbers but seems ok for
	// positive numbers needing more than 32 bit.
	switch v := value.(type) {
	case bool:
		a = zipkincore.AnnotationType_BOOL
		b = []byte("\x00")
		if v {
			b = []byte("\x01")
		}
	case []byte:
		a = zipkincore.AnnotationType_BYTES
		b = v
	case byte:
		a = zipkincore.AnnotationType_I32
		b = make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(v))
	case int8:
		a = zipkincore.AnnotationType_I32
		b = make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(v))
	case int16:
		a = zipkincore.AnnotationType_I32
		b = make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(v))
	case uint16:
		a = zipkincore.AnnotationType_I32
		b = make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(v))
	case int32:
		a = zipkincore.AnnotationType_I32
		b = make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(v))
	case uint32:
		a = zipkincore.AnnotationType_I32
		b = make([]byte, 4)
		binary.BigEndian.PutUint32(b, v)
	case int64:
		a = zipkincore.AnnotationType_I64
		b = make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(v))
	case int:
		a = zipkincore.AnnotationType_I32
		b = make([]byte, 8)
		binary.BigEndian.PutUint32(b, uint32(v))
	case uint:
		a = zipkincore.AnnotationType_I32
		b = make([]byte, 8)
		binary.BigEndian.PutUint32(b, uint32(v))
	case uint64:
		a = zipkincore.AnnotationType_I64
		b = make([]byte, 8)
		binary.BigEndian.PutUint64(b, v)
	case float32:
		a = zipkincore.AnnotationType_DOUBLE
		b = make([]byte, 8)
		bits := math.Float64bits(float64(v))
		binary.BigEndian.PutUint64(b, bits)
	case float64:
		a = zipkincore.AnnotationType_DOUBLE
		b = make([]byte, 8)
		bits := math.Float64bits(v)
		binary.BigEndian.PutUint64(b, bits)
	case string:
		a = zipkincore.AnnotationType_STRING
		b = []byte(v)
	default:
		// we have no handler for type's value, but let's get a string
		// representation of it.
		a = zipkincore.AnnotationType_STRING
		b = []byte(fmt.Sprintf("%+v", value))
	}
	span.BinaryAnnotations = append(span.BinaryAnnotations, &zipkincore.BinaryAnnotation{
		Key:            key,
		Value:          b,
		AnnotationType: a,
		Host:           host,
	})
}
