package querier

import (
	"context"
	"fmt"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

type lazyQuerier struct {
	next storage.Querier
}

// newLazyQuerier wraps a storage.Querier, does the Select in the background.
// Return value cannot be used from more than one goroutine simultaneously.
func newLazyQuerier(next storage.Querier) storage.Querier {
	return lazyQuerier{next}
}

func (l lazyQuerier) Select(params *storage.SelectParams, matchers ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	future := make(chan storage.SeriesSet)
	go func() {
		set, _, err := l.next.Select(params, matchers...)
		if err != nil {
			future <- errSeriesSet{err}
		} else {
			future <- set
		}
	}()
	return &lazySeriesSet{
		future: future,
	}, nil, nil
}

func (l lazyQuerier) LabelValues(name string) ([]string, error) {
	return l.next.LabelValues(name)
}

func (l lazyQuerier) LabelNames() ([]string, error) {
	return l.next.LabelNames()
}

func (l lazyQuerier) Close() error {
	return l.next.Close()
}

// Get implements ChunkStore for the chunk tar HTTP handler.
func (l lazyQuerier) Get(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]chunk.Chunk, error) {
	store, ok := l.next.(ChunkStore)
	if !ok {
		return nil, fmt.Errorf("not supported")
	}

	return store.Get(ctx, from, through, matchers...)
}

// errSeriesSet implements storage.SeriesSet, just returning an error.
type errSeriesSet struct {
	err error
}

func (errSeriesSet) Next() bool {
	return false
}

func (errSeriesSet) At() storage.Series {
	return nil
}

func (e errSeriesSet) Err() error {
	return e.err
}

type lazySeriesSet struct {
	next   storage.SeriesSet
	future chan storage.SeriesSet
}

// Next implements storage.SeriesSet.  NB not thread safe!
func (s *lazySeriesSet) Next() bool {
	if s.next == nil {
		s.next = <-s.future
	}
	return s.next.Next()
}

func (s lazySeriesSet) At() storage.Series {
	if s.next == nil {
		s.next = <-s.future
	}
	return s.next.At()
}

func (s lazySeriesSet) Err() error {
	if s.next == nil {
		s.next = <-s.future
	}
	return s.next.Err()
}
