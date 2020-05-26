package querier

import (
	"context"

	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

func newTracingSet(ctx context.Context, op string, set storage.SeriesSet) storage.SeriesSet {
	span, _ := opentracing.StartSpanFromContext(ctx, op)
	return &tracingSet{
		s:    set,
		span: span,
	}
}

type tracingSet struct {
	s    storage.SeriesSet
	span opentracing.Span

	series  int
	samples int
}

func (t *tracingSet) Next() bool {
	if t.s.Next() {
		t.series++
		return true
	}

	if t.span != nil {
		t.span.LogKV("series", t.series, "samples", t.samples)
		t.span.Finish()
		// Nil to avoid double closing.
		t.span = nil
	}
	return false
}

func (t *tracingSet) At() storage.Series {
	s := t.s.At()
	if t.span != nil && t.series%100 == 0 {
		t.span.LogKV("at", s.Labels().String(), "current series", t.series, "current samples", t.samples)
	}
	return &tracingSeries{t, s}
}

func (t *tracingSet) Err() error {
	return t.s.Err()
}

type tracingSeries struct {
	set *tracingSet
	s   storage.Series
}

func (t tracingSeries) Labels() labels.Labels {
	return t.s.Labels()
}

func (t tracingSeries) Iterator() chunkenc.Iterator {
	return tracingIterator{t.set, t.s.Iterator()}
}

type tracingIterator struct {
	set *tracingSet
	it  chunkenc.Iterator
}

func (t tracingIterator) Next() bool {
	if t.it.Next() {
		t.set.samples++
		return true
	}
	return false
}

func (t tracingIterator) Seek(ts int64) bool {
	if t.it.Seek(ts) {
		t.set.samples++
		return true
	}
	return false
}

func (t tracingIterator) At() (int64, float64) {
	return t.it.At()
}

func (t tracingIterator) Err() error {
	return t.it.Err()
}
