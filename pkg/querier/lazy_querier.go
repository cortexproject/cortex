package querier

import (
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

type lazyQuerier struct {
	next storage.Querier
}

func newLazyQuerier(next storage.Querier) storage.Querier {
	return lazyQuerier{next}
}

func (l lazyQuerier) Select(params *storage.SelectParams, matchers ...*labels.Matcher) (storage.SeriesSet, error) {
	future := make(chan storage.SeriesSet)
	go func() {
		set, err := l.next.Select(params, matchers...)
		if err != nil {
			future <- errSeriesSet{err}
		} else {
			future <- set
		}
	}()
	return &lazySeriesSet{
		future: future,
	}, nil
}

func (l lazyQuerier) LabelValues(name string) ([]string, error) {
	return l.next.LabelValues(name)
}

func (l lazyQuerier) Close() error {
	return l.next.Close()
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
