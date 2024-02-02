// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package ringbuffer

import "math"

type Sample[T any] struct {
	T int64
	V T
}

type RingBuffer[T any] struct {
	items []Sample[T]
	tail  []Sample[T]
}

func New[T any](size int) *RingBuffer[T] {
	return &RingBuffer[T]{
		items: make([]Sample[T], 0, size),
	}
}

func (r *RingBuffer[T]) Len() int {
	return len(r.items)
}

// MaxT returns the maximum timestamp of the ring buffer.
// If the ring buffer is empty, it returns math.MinInt64.
func (r *RingBuffer[T]) MaxT() int64 {
	if len(r.items) == 0 {
		return math.MinInt64
	}
	return r.items[len(r.items)-1].T
}

// ReadIntoNext can be used to read a sample into the next ring buffer slot through the passed in callback.
// If the callback function returns false, the sample is not kept in the buffer.
func (r *RingBuffer[T]) ReadIntoNext(f func(*Sample[T]) bool) {
	n := len(r.items)
	if cap(r.items) > len(r.items) {
		r.items = r.items[:n+1]
	} else {
		r.items = append(r.items, Sample[T]{})
	}
	if keep := f(&r.items[n]); !keep {
		r.items = r.items[:n]
	}
}

func (r *RingBuffer[T]) ReadIntoLast(f func(*Sample[T])) {
	f(&r.items[len(r.items)-1])
}

func (r *RingBuffer[T]) Push(t int64, v T) {
	if n := len(r.items); n < cap(r.items) {
		r.items = r.items[:n+1]
		r.items[n].T = t
		r.items[n].V = v
	} else {
		r.items = append(r.items, Sample[T]{T: t, V: v})
	}
}

func (r *RingBuffer[T]) DropBefore(ts int64) {
	if len(r.items) == 0 || r.items[len(r.items)-1].T < ts {
		r.items = r.items[:0]
		return
	}
	var drop int
	for drop = 0; drop < len(r.items) && r.items[drop].T < ts; drop++ {
	}
	keep := len(r.items) - drop

	r.tail = resize(r.tail, drop)
	copy(r.tail, r.items[:drop])
	copy(r.items, r.items[drop:])
	copy(r.items[keep:], r.tail)
	r.items = r.items[:keep]
}

func (r *RingBuffer[T]) DropBeforeWithExtLookback(ts int64, extMint int64) {
	if len(r.items) == 0 || r.items[len(r.items)-1].T < ts {
		r.items = r.items[:0]
		return
	}
	var drop int
	for drop = 0; drop < len(r.items) && r.items[drop].T < ts; drop++ {
	}
	if drop > 0 && r.items[drop-1].T >= extMint {
		drop--
	}
	keep := len(r.items) - drop

	r.tail = resize(r.tail, drop)
	copy(r.tail, r.items[:drop])
	copy(r.items, r.items[drop:])
	copy(r.items[keep:], r.tail)
	r.items = r.items[:keep]
}

func (r *RingBuffer[T]) Samples() []Sample[T] {
	return r.items
}

func resize[T any](s []Sample[T], n int) []Sample[T] {
	if cap(s) >= n {
		return s[:n]
	}
	return make([]Sample[T], n)
}
