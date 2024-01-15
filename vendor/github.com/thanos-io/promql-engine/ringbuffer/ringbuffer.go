// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package ringbuffer

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

func (r *RingBuffer[T]) MaxT() int64 {
	return r.items[len(r.items)-1].T
}

func (r *RingBuffer[T]) ReadIntoNext(f func(*Sample[T])) {
	n := len(r.items)
	if cap(r.items) > len(r.items) {
		r.items = r.items[:n+1]
	} else {
		r.items = append(r.items, Sample[T]{})
	}
	f(&r.items[n])
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

func (r *RingBuffer[T]) Samples() []Sample[T] {
	return r.items
}

func resize[T any](s []Sample[T], n int) []Sample[T] {
	if cap(s) >= n {
		return s[:n]
	}
	return make([]Sample[T], n)
}
