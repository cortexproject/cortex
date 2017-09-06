package loki

import (
	"sync"

	"github.com/weaveworks-experiments/loki/pkg/model"
)

// Want to be able to support a service doing 100 QPS with a 15s scrape interval
var globalCollector = NewCollector(15 * 100)

type Collector struct {
	mtx      sync.Mutex
	traceIDs map[uint64]int // map from trace ID to index in traces
	traces   []model.Trace
	next     int
	length   int
}

func NewCollector(capacity int) *Collector {
	return &Collector{
		traceIDs: make(map[uint64]int, capacity),
		traces:   make([]model.Trace, capacity, capacity),
		next:     0,
		length:   0,
	}
}

func (c *Collector) Collect(span model.Span) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	traceID := span.TraceId
	idx, ok := c.traceIDs[traceID]
	if !ok {
		// Pick a slot in c.spans for this trace
		idx = c.next
		c.next++
		c.next %= cap(c.traces) // wrap

		// If the slot it occupied, we'll need to clear the trace ID index,
		// otherwise we'll need to number of traces.
		if c.length == cap(c.traces) {
			delete(c.traceIDs, c.traces[idx].TraceId)
		} else {
			c.length++
		}

		// Initialise said slot.
		c.traceIDs[traceID] = idx
		c.traces[idx].TraceId = traceID
		c.traces[idx].Spans = c.traces[idx].Spans[:0]
	}

	c.traces[idx].Spans = append(c.traces[idx].Spans, span)
	return nil
}

func (*Collector) Close() error {
	return nil
}

func (c *Collector) gather() []model.Trace {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	traces := make([]model.Trace, 0, c.length)
	i, count := c.next-c.length, 0
	if i < 0 {
		i = cap(c.traces) + i
	}
	for count < c.length {
		i %= cap(c.traces)
		traces = append(traces, c.traces[i])
		i++
		count++
	}
	return traces
}
