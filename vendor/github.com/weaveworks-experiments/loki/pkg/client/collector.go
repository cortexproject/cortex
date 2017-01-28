package loki

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/openzipkin/zipkin-go-opentracing/_thrift/gen-go/zipkincore"
)

var globalCollector = NewCollector(1024)

type Collector struct {
	mtx    sync.Mutex
	spans  []*zipkincore.Span
	next   int
	length int
}

func NewCollector(capacity int) *Collector {
	return &Collector{
		spans:  make([]*zipkincore.Span, capacity, capacity),
		next:   0,
		length: 0,
	}
}

func (c *Collector) Collect(span *zipkincore.Span) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	c.spans[c.next] = span
	c.next++
	c.next %= cap(c.spans) // wrap

	if c.length < cap(c.spans) {
		c.length++
	}

	return nil
}

func (*Collector) Close() error {
	return nil
}

func (c *Collector) gather() []*zipkincore.Span {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	spans := make([]*zipkincore.Span, 0, c.length)
	for i := 0; i < c.length; i++ {
		idx := (c.next - c.length + i) % cap(c.spans)
		if idx < 0 {
			idx = cap(c.spans) + idx
		}
		spans = append(spans, c.spans[idx])
		c.spans[idx] = nil
	}
	return spans
}

func (c *Collector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	spans := c.gather()
	if err := WriteSpans(spans, w); err != nil {
		log.Printf("error writing spans: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func WriteSpans(spans []*zipkincore.Span, w io.Writer) error {
	transport := thrift.NewStreamTransportW(w)
	protocol := thrift.NewTCompactProtocol(transport)

	if err := protocol.WriteListBegin(thrift.STRUCT, len(spans)); err != nil {
		return err
	}
	for _, span := range spans {
		if err := span.Write(protocol); err != nil {
			return err
		}
	}
	if err := protocol.WriteListEnd(); err != nil {
		return err
	}
	return protocol.Flush()
}

func ReadSpans(r io.Reader) ([]*zipkincore.Span, error) {
	transport := thrift.NewStreamTransportR(r)
	protocol := thrift.NewTCompactProtocol(transport)
	ttype, size, err := protocol.ReadListBegin()
	if err != nil {
		return nil, err
	}
	spans := make([]*zipkincore.Span, 0, size)
	if ttype != thrift.STRUCT {
		return nil, fmt.Errorf("unexpected type: %v", ttype)
	}
	for i := 0; i < size; i++ {
		span := zipkincore.NewSpan()
		if err := span.Read(protocol); err != nil {
			return nil, err
		}
		spans = append(spans, span)
	}
	return spans, protocol.ReadListEnd()
}

func Handler() http.Handler {
	return globalCollector
}
