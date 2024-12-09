package util

import (
	"context"
	"sync"
	"time"
	"unsafe"

	"github.com/bboreham/go-loser"
	"github.com/hashicorp/golang-lru/v2/expirable"
)

const (
	// Max size is ser to 2M.
	maxInternerLruCacheSize = 2e6
	// TTL should be similar to the head compaction interval
	internerLruCacheTTL = time.Hour * 2
)

// StringsContain returns true if the search value is within the list of input values.
func StringsContain(values []string, search string) bool {
	for _, v := range values {
		if search == v {
			return true
		}
	}

	return false
}

// StringsMap returns a map where keys are input values.
func StringsMap(values []string) map[string]bool {
	out := make(map[string]bool, len(values))
	for _, v := range values {
		out[v] = true
	}
	return out
}

// StringsClone returns a copy input s
// see: https://github.com/golang/go/blob/master/src/strings/clone.go
func StringsClone(s string) string {
	b := make([]byte, len(s))
	copy(b, s)
	return *(*string)(unsafe.Pointer(&b))
}

// MergeSlicesParallel merge sorted slices in parallel
// using the MergeSortedSlices function
func MergeSlicesParallel(ctx context.Context, parallelism int, a ...[]string) ([]string, error) {
	if parallelism <= 1 {
		return MergeSortedSlices(ctx, a...)
	}
	if len(a) == 0 {
		return nil, nil
	}
	if len(a) == 1 {
		return a[0], nil
	}
	c := make(chan []string, len(a))
	errCh := make(chan error, 1)
	wg := sync.WaitGroup{}
	var r [][]string
	p := min(parallelism, len(a)/2)
	batchSize := len(a) / p

	for i := 0; i < len(a); i += batchSize {
		wg.Add(1)
		go func(i int) {
			m := min(len(a), i+batchSize)
			r, e := MergeSortedSlices(ctx, a[i:m]...)
			if e != nil {
				errCh <- e
				wg.Done()
				return
			}
			c <- r
			wg.Done()
		}(i)
	}

	go func() {
		wg.Wait()
		close(c)
		close(errCh)
	}()

	if err := <-errCh; err != nil {
		return nil, err
	}
	for s := range c {
		r = append(r, s)
	}

	return MergeSortedSlices(ctx, r...)
}

func NewStringListIter(s []string) *StringListIter {
	return &StringListIter{l: s}
}

type StringListIter struct {
	l   []string
	cur string
}

func (s *StringListIter) Next() bool {
	if len(s.l) == 0 {
		return false
	}
	s.cur = s.l[0]
	s.l = s.l[1:]
	return true
}

func (s *StringListIter) At() string { return s.cur }

var MAX_STRING = string([]byte{0xff})

// MergeSortedSlices merges a set of sorted string slices into a single ones
// while removing all duplicates.
func MergeSortedSlices(ctx context.Context, a ...[]string) ([]string, error) {
	if len(a) == 1 {
		return a[0], nil
	}
	its := make([]*StringListIter, 0, len(a))
	sumLengh := 0
	for _, s := range a {
		sumLengh += len(s)
		its = append(its, NewStringListIter(s))
	}
	lt := loser.New(its, MAX_STRING)

	if sumLengh == 0 {
		return []string{}, nil
	}

	r := make([]string, 0, sumLengh*2/10)
	var current string
	cnt := 0
	for lt.Next() {
		cnt++
		if cnt%CheckContextEveryNIterations == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if lt.At() != current {
			current = lt.At()
			r = append(r, current)
		}
	}
	return r, nil
}

type Interner interface {
	Intern(s string) string
}

// NewLruInterner returns a new Interner to be used to intern strings.
// The interner will use a LRU cache to return the deduplicated strings
func NewLruInterner() Interner {
	return &pool{
		lru: expirable.NewLRU[string, string](maxInternerLruCacheSize, nil, internerLruCacheTTL),
	}
}

type pool struct {
	lru *expirable.LRU[string, string]
}

// Intern returns the interned string. It returns the canonical representation of string.
func (p *pool) Intern(s string) string {
	if s == "" {
		return ""
	}

	interned, ok := p.lru.Get(s)
	if ok {
		return interned
	}
	p.lru.Add(s, s)
	return s
}
