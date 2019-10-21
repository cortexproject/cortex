package lazyquery

import (
	"context"
	"fmt"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/querier/chunkstore"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

type LazyQueryable struct {
	q storage.Queryable
}

func (lq LazyQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	q, err := lq.q.Querier(ctx, mint, maxt)
	if err != nil {
		return nil, err
	}

	return NewLazyQuerier(q), nil
}

func NewLazyQueryable(q storage.Queryable) storage.Queryable {
	return LazyQueryable{q}
}

type LazyQuerier struct {
	next storage.Querier
}

// NewLazyQuerier wraps a storage.Querier, does the Select in the background.
// Return value cannot be used from more than one goroutine simultaneously.
func NewLazyQuerier(next storage.Querier) storage.Querier {
	return LazyQuerier{next}
}

func (l LazyQuerier) Select(params *storage.SelectParams, matchers ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
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

func (l LazyQuerier) LabelValues(name string) ([]string, storage.Warnings, error) {
	return l.next.LabelValues(name)
}

func (l LazyQuerier) LabelNames() ([]string, storage.Warnings, error) {
	return l.next.LabelNames()
}

func (l LazyQuerier) Close() error {
	return l.next.Close()
}

// Get implements ChunkStore for the chunk tar HTTP handler.
func (l LazyQuerier) Get(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) ([]chunk.Chunk, error) {
	store, ok := l.next.(chunkstore.ChunkStore)
	if !ok {
		return nil, fmt.Errorf("not supported")
	}

	return store.Get(ctx, userID, from, through, matchers...)
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
