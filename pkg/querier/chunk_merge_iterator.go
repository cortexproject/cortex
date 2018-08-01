package querier

import (
	"container/heap"
	"sort"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage"
	"github.com/weaveworks/cortex/pkg/chunk"
)

// Limit on the window size of seeks.
const window = 24 * time.Hour
const chunkSize = 12 * time.Hour

type chunkMergeIterator struct {
	chunks []*chunkIterator
	h      seriesIteratorHeap

	curr    *chunkIterator
	lastErr error
}

func newChunkMergeIterator(cs []chunk.Chunk) storage.SeriesIterator {
	chunks := make([]*chunkIterator, len(cs), len(cs))
	for i := range cs {
		chunks[i] = &chunkIterator{
			Chunk: cs[i],
			it:    cs[i].Data.NewIterator(),
		}
	}
	sort.Sort(byFrom(chunks))

	c := &chunkMergeIterator{
		chunks: chunks,
		h:      make(seriesIteratorHeap, 0, len(chunks)),
	}

	for _, iter := range c.chunks {
		if iter.Next() {
			heap.Push(&c.h, iter)
		} else if err := iter.Err(); err != nil {
			c.lastErr = err
		}
	}
	return c
}

func (c *chunkMergeIterator) findChunks(t int64) []*chunkIterator {
	// Find beginning and end index into list of chunks.
	i := sort.Search(len(c.chunks), func(i int) bool {
		return c.chunks[i].From.Add(chunkSize) >= model.Time(t)
	})
	j := sort.Search(len(c.chunks), func(i int) bool {
		return model.Time(t).Add(window) <= c.chunks[i].From
	})
	return c.chunks[i:j]
}

func (c *chunkMergeIterator) Seek(t int64) bool {
	chunks := c.findChunks(t)
	c.curr = nil
	c.h = c.h[:0]

	for _, iter := range chunks {
		if iter.Seek(t) {
			heap.Push(&c.h, iter)
		} else if err := iter.Err(); err != nil {
			c.lastErr = err
			return false
		}
	}

	return c.popAndDedupe()
}

func (c *chunkMergeIterator) Next() bool {
	if c.curr != nil {
		if c.curr.Next() {
			heap.Push(&c.h, c.curr)
		} else if err := c.curr.Err(); err != nil {
			c.lastErr = err
			return false
		}
		c.curr = nil
	}

	return c.popAndDedupe()
}

func (c *chunkMergeIterator) popAndDedupe() bool {
	if len(c.h) == 0 {
		return false
	}

	c.curr = heap.Pop(&c.h).(*chunkIterator)
	for len(c.h) > 0 {
		next := c.h[0]
		nT, _ := next.At()
		cT, _ := c.curr.At()
		if nT != cT {
			break
		}

		if next.Next() {
			heap.Fix(&c.h, 0)
			continue
		}

		heap.Pop(&c.h)
		if err := next.Err(); err != nil {
			c.lastErr = err
			return false
		}
	}
	return true
}

func (c *chunkMergeIterator) At() (t int64, v float64) {
	if c.curr == nil {
		panic("mergeIterator.At() called after .Next() returned false.")
	}

	return c.curr.At()
}

func (c *chunkMergeIterator) Err() error {
	return c.lastErr
}

type seriesIteratorHeap []storage.SeriesIterator

func (h seriesIteratorHeap) Len() int      { return len(h) }
func (h seriesIteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h seriesIteratorHeap) Less(i, j int) bool {
	iT, _ := h[i].At()
	jT, _ := h[j].At()
	return iT < jT
}

func (h *seriesIteratorHeap) Push(x interface{}) {
	*h = append(*h, x.(storage.SeriesIterator))
}

func (h *seriesIteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type byFrom []*chunkIterator

func (b byFrom) Len() int           { return len(b) }
func (b byFrom) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byFrom) Less(i, j int) bool { return b[i].From < b[j].From }

type nonOverlappingIterator struct {
	its []*chunkIterator

	curr *chunkIterator
}

// newNonOverlappingIterator returns a single iterator over an slice of sorted,
// non-overlapping iterators.
func newNonOverlappingIterator(its []*chunkIterator) *nonOverlappingIterator {
	return &nonOverlappingIterator{
		its: its,
	}
}

func (it *nonOverlappingIterator) Seek(t int64) bool {
	for it.curr == nil || !it.curr.Seek(t) {
		if len(it.its) == 0 {
			return false
		}

		it.curr = it.its[0]
		it.its = it.its[1:]
	}

	return true
}

func (it *nonOverlappingIterator) Next() bool {
	for it.curr == nil || !it.curr.Next() {
		if len(it.its) == 0 {
			return false
		}

		it.curr = it.its[0]
		it.its = it.its[1:]
	}

	return true
}

func (it *nonOverlappingIterator) At() (int64, float64) {
	return it.curr.At()
}

func (it *nonOverlappingIterator) Err() error {
	if it.curr == nil {
		return nil
	}

	return it.curr.Err()
}

type chunkMergeIteratorV2 struct {
	its []*nonOverlappingIterator
	h   seriesIteratorHeap

	curr    *nonOverlappingIterator
	lastErr error
}

func newChunkMergeIteratorV2(cs []chunk.Chunk) storage.SeriesIterator {
	chunks := make([]*chunkIterator, len(cs))
	for i := range cs {
		chunks[i] = &chunkIterator{
			Chunk: cs[i],
			it:    cs[i].Data.NewIterator(),
		}
	}
	sort.Sort(byFrom(chunks))

	chkLists := [][]*chunkIterator{}
Outer:
	for _, chk := range chunks {
		for i, chkList := range chkLists {
			if chkList[len(chkList)-1].Through.Before(chk.From) {
				chkLists[i] = append(chkLists[i], chk)
				continue Outer
			}
		}

		// Here only if there is no list that accepted this chunk. Need a new one.
		chkLists = append(chkLists, []*chunkIterator{chk})
	}

	its := make([]*nonOverlappingIterator, 0, len(chkLists))
	for _, chkList := range chkLists {
		its = append(its, newNonOverlappingIterator(chkList))
	}

	c := &chunkMergeIteratorV2{
		its: its,
		h:   make(seriesIteratorHeap, 0, len(chunks)),
	}

	for _, iter := range c.its {
		if iter.Next() {
			heap.Push(&c.h, iter)
		} else if err := iter.Err(); err != nil {
			c.lastErr = err
		}
	}
	return c
}

func (c *chunkMergeIteratorV2) Seek(t int64) bool {
	c.h = c.h[:0]

	for _, iter := range c.its {
		if iter.Seek(t) {
			heap.Push(&c.h, iter)
		} else if err := iter.Err(); err != nil {
			c.lastErr = err
			return false
		}
	}

	return c.popAndDedupe()
}

func (c *chunkMergeIteratorV2) Next() bool {
	if c.curr != nil {
		if c.curr.Next() {
			heap.Push(&c.h, c.curr)
		} else if err := c.curr.Err(); err != nil {
			c.lastErr = err
			return false
		}
		c.curr = nil
	}

	return c.popAndDedupe()
}

func (c *chunkMergeIteratorV2) popAndDedupe() bool {
	if len(c.h) == 0 {
		return false
	}

	c.curr = heap.Pop(&c.h).(*nonOverlappingIterator)
	for len(c.h) > 0 {
		next := c.h[0]
		nT, _ := next.At()
		cT, _ := c.curr.At()
		if nT != cT {
			break
		}

		if next.Next() {
			heap.Fix(&c.h, 0)
			continue
		}

		heap.Pop(&c.h)
		if err := next.Err(); err != nil {
			c.lastErr = err
			return false
		}
	}

	return true
}

func (c *chunkMergeIteratorV2) At() (t int64, v float64) {
	if c.curr == nil {
		panic("mergeIterator.At() called after .Next() returned false.")
	}

	return c.curr.At()
}

func (c *chunkMergeIteratorV2) Err() error {
	return c.lastErr
}
