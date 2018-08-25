package batch

import (
	"container/heap"
	"sort"

	"github.com/weaveworks/cortex/pkg/chunk"
	promchunk "github.com/weaveworks/cortex/pkg/prom1/storage/local/chunk"
)

type batchMergeIterator struct {
	its []*nonOverlappingIterator
	h   batchIteratorHeap

	// Store the current sorted batchStream
	batches batchStream

	// For the next set of batches we'll collect, and buffers to merge them in.
	nextBatches    batchStream
	nextBatchesBuf batchStream
	batchesBuf     batchStream

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

		// TODO its 2x # iterators guaranteed to be enough?
		batches:        make(batchStream, 0, len(its)*2),
		nextBatches:    make(batchStream, 0, len(its)*2),
		nextBatchesBuf: make(batchStream, 0, len(its)*2),
		batchesBuf:     make(batchStream, 0, len(its)*2),
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
	// Pop the last built batch in a way that doesn't extend the slice.
	if len(c.batches) > 0 {
		copy(c.batches, c.batches[1:])
		c.batches = c.batches[:len(c.batches)-1]
	}

	c.buildNextBatch()
	return len(c.batches) > 0
}

func (c *batchMergeIterator) buildNextBatch() {
	// Must always consider #iters * batchsize samples, to ensure
	// we have the opportunity to dedupe samples without missing any.
	required := len(c.h) * promchunk.BatchSize
	for i := range c.batches {
		required -= c.batches[i].Length - c.batches[i].Index
	}

	c.nextBatches = c.nextBatches[:0]
	for required > 0 && len(c.h) > 0 {
		// Optimisation: if we have at least one batches, and the next batch starts
		// after the last batch we have, we have no overlaps and can break early.
		// if len(c.batches) > 0 {
		// 	last := &c.batches[len(c.batches)-1]
		// 	if last.timestamps[last.length-1] < c.h[0].AtTime() {
		// 		break
		// 	}
		// }

		b := c.h[0].Batch()
		required -= b.Length
		c.nextBatches = append(c.nextBatches, b)
		if c.h[0].Next() {
			heap.Fix(&c.h, 0)
		} else {
			heap.Pop(&c.h)
		}
	}

	if len(c.nextBatches) > 0 {
		c.nextBatchesBuf = mergeBatches(c.nextBatches, c.nextBatchesBuf[:len(c.nextBatches)])
		c.batchesBuf = mergeStreams(c.batches, c.nextBatchesBuf, c.batchesBuf)
		copy(c.batches[:len(c.batchesBuf)], c.batchesBuf)
		c.batches = c.batches[:len(c.batchesBuf)]
	}
}

func (c *batchMergeIterator) AtTime() int64 {
	return c.batches[0].Timestamps[0]
}

func (c *batchMergeIterator) Batch() promchunk.Batch {
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
		cs := make([]chunk.Chunk, 0, len(cs)/(len(css)+1))
		cs = append(cs, c)
		css = append(css, cs)
	}

	return css
}

type byFrom []chunk.Chunk

func (b byFrom) Len() int           { return len(b) }
func (b byFrom) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byFrom) Less(i, j int) bool { return b[i].From < b[j].From }
