package util

import (
	"sync"
	"unsafe"

	"github.com/bboreham/go-loser"
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
func MergeSlicesParallel(parallelism int, a ...[]string) []string {
	if parallelism <= 1 {
		return MergeSortedSlices(a...)
	}
	if len(a) == 0 {
		return nil
	}
	if len(a) == 1 {
		return a[0]
	}
	c := make(chan []string, len(a))
	wg := sync.WaitGroup{}
	var r [][]string
	p := min(parallelism, len(a)/2)
	batchSize := len(a) / p

	for i := 0; i < len(a); i += batchSize {
		wg.Add(1)
		go func(i int) {
			m := min(len(a), i+batchSize)
			c <- MergeSortedSlices(a[i:m]...)
			wg.Done()
		}(i)
	}

	go func() {
		wg.Wait()
		close(c)
	}()

	for s := range c {
		r = append(r, s)
	}

	return MergeSortedSlices(r...)
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
func MergeSortedSlices(a ...[]string) []string {
	if len(a) == 1 {
		return a[0]
	}
	its := make([]*StringListIter, 0, len(a))
	sumLengh := 0
	for _, s := range a {
		sumLengh += len(s)
		its = append(its, NewStringListIter(s))
	}
	lt := loser.New(its, MAX_STRING)

	if sumLengh == 0 {
		return []string{}
	}

	r := make([]string, 0, sumLengh*2/10)
	var current string
	for lt.Next() {
		if lt.At() != current {
			current = lt.At()
			r = append(r, current)
		}
	}
	return r
}
