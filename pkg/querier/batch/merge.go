package batch

import (
	"container/heap"
	"sort"

	"github.com/weaveworks/cortex/pkg/chunk"
)

type batchMergeIterator struct {
	its []*nonOverlappingIterator
	h   batchIteratorHeap

	batches []batch
	currErr error
}

func newBatchMergeIterator(cs []chunk.Chunk) *batchMergeIterator {
	css := partitionChunks(cs)
	its := make([]*nonOverlappingIterator, 0, len(css))
	for _, cs := range css {
		its = append(its, newNonOverlappingIterator(cs))
	}

	c := &batchMergeIterator{
		its: its,
		h:   make(batchIteratorHeap, 0, len(its)),
	}

	for _, iter := range c.its {
		if iter.Next() {
			c.h = append(c.h, iter)
			continue
		}

		if err := iter.Err(); err != nil {
			c.currErr = err
		}
	}

	heap.Init(&c.h)
	return c
}

func (c *batchMergeIterator) Seek(t int64) bool {
	c.h = c.h[:0]
	c.batches = c.batches[:0]

	for _, iter := range c.its {
		if iter.Seek(t) {
			c.h = append(c.h, iter)
			continue
		}

		if err := iter.Err(); err != nil {
			c.currErr = err
			return false
		}
	}

	heap.Init(&c.h)
	c.buildNextBatch()
	return len(c.batches) > 0
}

func (c *batchMergeIterator) Next() bool {
	// pop the last built batch
	if len(c.batches) > 0 {
		c.batches = c.batches[1:]
	}

	c.buildNextBatch()
	return len(c.batches) > 0
}

func (c *batchMergeIterator) buildNextBatch() {
	unique := 0
	for i := range c.batches {
		unique += c.batches[i].length
	}
	required := (((len(c.h) * batchSize) - unique) / batchSize) + 1
	if required <= 0 {
		return
	}

	batches := make([]batch, 0, len(c.h))
	for len(batches) < required && len(c.h) > 0 {
		batches = append(batches, c.h[0].Batch())
		if c.h[0].Next() {
			heap.Fix(&c.h, 0)
		} else {
			heap.Pop(&c.h)
		}
	}

	merged := make([]batch, len(batches))
	merged = mergeBatches(batches, merged)
	output := make([]batch, len(c.batches)+len(merged))
	c.batches = mergeStreams(c.batches, merged, output[:0])
}

func (c *batchMergeIterator) AtTime() int64 {
	return c.batches[0].timestamps[0]
}

func (c *batchMergeIterator) Batch() batch {
	return c.batches[0]
}

func (c *batchMergeIterator) Err() error {
	return c.currErr
}

type batchIteratorHeap []batchIterator

func (h *batchIteratorHeap) Len() int      { return len(*h) }
func (h *batchIteratorHeap) Swap(i, j int) { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }

func (h *batchIteratorHeap) Less(i, j int) bool {
	iT := (*h)[i].AtTime()
	jT := (*h)[j].AtTime()
	return iT < jT
}

func (h *batchIteratorHeap) Push(x interface{}) {
	*h = append(*h, x.(batchIterator))
}

func (h *batchIteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Build a list of lists of non-overlapping chunks.
func partitionChunks(cs []chunk.Chunk) [][]chunk.Chunk {
	sort.Sort(byFrom(cs))

	css := [][]chunk.Chunk{}
outer:
	for _, c := range cs {
		for i, cs := range css {
			if cs[len(cs)-1].Through.Before(c.From) {
				css[i] = append(css[i], c)
				continue outer
			}
		}
		css = append(css, []chunk.Chunk{c})
	}

	return css
}

type byFrom []chunk.Chunk

func (b byFrom) Len() int           { return len(b) }
func (b byFrom) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byFrom) Less(i, j int) bool { return b[i].From < b[j].From }
