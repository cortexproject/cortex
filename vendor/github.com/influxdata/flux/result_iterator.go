package flux

import (
	"sort"
)

// ResultIterator allows iterating through all results synchronously.
// A ResultIterator is not thread-safe and all of the methods are expected to be
// called within the same goroutine.
type ResultIterator interface {
	// More indicates if there are more results.
	More() bool

	// Next returns the next result.
	// If More is false, Next panics.
	Next() Result

	// Release discards the remaining results and frees the currently used resources.
	// It must always be called to free resources. It can be called even if there are
	// more results. It is safe to call Release multiple times.
	Release()

	// Err reports the first error encountered.
	// Err will not report anything unless More has returned false,
	// or the query has been cancelled.
	Err() error

	// Statistics reports the statistics for the query.
	// The statistics are not complete until Release is called.
	Statistics() Statistics
}

// queryResultIterator implements a ResultIterator while consuming a Query
type queryResultIterator struct {
	query      Query
	released   bool
	nextResult Result
}

func NewResultIteratorFromQuery(q Query) ResultIterator {
	return &queryResultIterator{
		query: q,
	}
}

// More returns true iff there is more data to be produced by the iterator.
// More is idempotent---successive calls to More with no intervening call to Next should
// return the same value and leave the iterator in the same state.
func (r *queryResultIterator) More() bool {

	// When the return value is true, r.nextResult should be non-nil, and nil otherwise.

	if r.released {
		return false
	}

	if r.nextResult != nil {
		return true
	}

	nr, ok := <-r.query.Results()
	if !ok {
		r.nextResult = nil
		return false
	}

	r.nextResult = nr
	return true
}

// Next produces the next result.
// If there is no more data, Next panics.
// It is possible to call Next without calling More first (although not recommended).
func (r *queryResultIterator) Next() Result {
	if r.released {
		panic("call to Next() on released iterator")
	}
	var nr Result
	if r.nextResult == nil {
		var ok bool
		nr, ok = <-r.query.Results()
		if !ok {
			panic("call to Next() when More() is false")
		}
	} else {
		nr = r.nextResult
	}

	r.nextResult = nil
	return nr
}

// Release frees resources associated with this iterator.
func (r *queryResultIterator) Release() {
	r.query.Done()
	r.released = true
	r.nextResult = nil // a panic will occur if caller attempts to call Next().
}

func (r *queryResultIterator) Err() error {
	return r.query.Err()
}

func (r *queryResultIterator) Statistics() Statistics {
	return r.query.Statistics()
}

type mapResultIterator struct {
	results map[string]Result
	order   []string
}

func NewMapResultIterator(results map[string]Result) ResultIterator {
	order := make([]string, 0, len(results))
	for k := range results {
		order = append(order, k)
	}
	sort.Strings(order)
	return &mapResultIterator{
		results: results,
		order:   order,
	}
}

func (r *mapResultIterator) More() bool {
	return len(r.order) > 0
}

func (r *mapResultIterator) Next() Result {
	next := r.order[0]
	r.order = r.order[1:]
	return r.results[next]
}

func (r *mapResultIterator) Release() {
	r.results = nil
	r.order = nil
}

func (r *mapResultIterator) Err() error {
	return nil
}

func (r *mapResultIterator) Statistics() Statistics {
	return Statistics{}
}

type sliceResultIterator struct {
	i       int
	results []Result
}

func NewSliceResultIterator(results []Result) ResultIterator {
	return &sliceResultIterator{
		results: results,
	}
}

func (r *sliceResultIterator) More() bool {
	return r.i < len(r.results)
}

func (r *sliceResultIterator) Next() Result {
	next := r.results[r.i]
	r.i++
	return next
}

func (r *sliceResultIterator) Release() {
	r.results, r.i = nil, 0
}

func (r *sliceResultIterator) Err() error {
	return nil
}

func (r *sliceResultIterator) Statistics() Statistics {
	return Statistics{}
}
