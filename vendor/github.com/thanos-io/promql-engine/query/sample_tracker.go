// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"fmt"
	"math"
	"sync/atomic"
)

type SampleTracker interface {
	Add(count int)
	Remove(count int)
	CheckLimit() error
	Limit() int64
}

type sampleTracker struct {
	current atomic.Int64
	limit   int64
}

func NewSampleTracker(maxSamples int) SampleTracker {
	if maxSamples <= 0 {
		return nopSampleTracker{}
	}
	return &sampleTracker{
		limit: int64(maxSamples),
	}
}

func (st *sampleTracker) Add(count int) {
	st.current.Add(int64(count))
}

func (st *sampleTracker) Remove(count int) {
	st.current.Add(-int64(count))
}

func (st *sampleTracker) CheckLimit() error {
	current := st.current.Load()
	if current > st.limit {
		return ErrMaxSamplesExceeded{Current: current, Limit: st.limit}
	}
	return nil
}

func (st *sampleTracker) Limit() int64 {
	return st.limit
}

type nopSampleTracker struct{}

func (nopSampleTracker) Add(int)           {}
func (nopSampleTracker) Remove(int)        {}
func (nopSampleTracker) CheckLimit() error { return nil }
func (nopSampleTracker) Limit() int64      { return math.MaxInt64 }

type ErrMaxSamplesExceeded struct {
	Current int64
	Limit   int64
}

func (e ErrMaxSamplesExceeded) Error() string {
	return fmt.Sprintf("query processing would load too many samples into memory: current=%d, limit=%d", e.Current, e.Limit)
}
