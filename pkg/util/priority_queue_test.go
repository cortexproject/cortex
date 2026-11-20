package util

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type simpleItem int64

func (i simpleItem) Priority() int64 {
	return int64(i)
}

func TestPriorityQueueBasic(t *testing.T) {
	queue := NewPriorityQueue(nil)
	assert.Equal(t, 0, queue.Length(), "Expected length = 0")

	queue.Enqueue(simpleItem(1))
	assert.Equal(t, 1, queue.Length(), "Expected length = 1")

	i, ok := queue.Dequeue().(simpleItem)
	assert.True(t, ok, "Expected cast to succeed")
	assert.Equal(t, simpleItem(1), i, "Expected to dequeue simpleItem(1)")

	queue.Close()
	assert.Nil(t, queue.Dequeue(), "Expect nil dequeue")
}

func TestPriorityQueuePriorities(t *testing.T) {
	queue := NewPriorityQueue(nil)
	queue.Enqueue(simpleItem(1))
	queue.Enqueue(simpleItem(2))
	queue.Enqueue(simpleItem(2))

	assert.Equal(t, simpleItem(2), queue.Peek().(simpleItem), "Expected to peek simpleItem(2)")
	assert.Equal(t, simpleItem(2), queue.Dequeue().(simpleItem), "Expected to dequeue simpleItem(2)")
	assert.Equal(t, simpleItem(2), queue.Dequeue().(simpleItem), "Expected to dequeue simpleItem(2)")
	assert.Equal(t, simpleItem(1), queue.Dequeue().(simpleItem), "Expected to dequeue simpleItem(1)")

	queue.Close()
	assert.Nil(t, queue.Dequeue(), "Expect nil dequeue")
}

func TestPriorityQueuePriorities2(t *testing.T) {
	queue := NewPriorityQueue(nil)
	queue.Enqueue(simpleItem(2))
	queue.Enqueue(simpleItem(1))
	queue.Enqueue(simpleItem(2))

	assert.Equal(t, simpleItem(2), queue.Peek().(simpleItem), "Expected to peek simpleItem(2)")
	assert.Equal(t, simpleItem(2), queue.Dequeue().(simpleItem), "Expected to dequeue simpleItem(2)")
	assert.Equal(t, simpleItem(2), queue.Dequeue().(simpleItem), "Expected to dequeue simpleItem(2)")
	assert.Equal(t, simpleItem(1), queue.Dequeue().(simpleItem), "Expected to dequeue simpleItem(1)")

	queue.Close()
	assert.Nil(t, queue.Dequeue(), "Expect nil dequeue")
}

func TestPriorityQueueWait(t *testing.T) {
	queue := NewPriorityQueue(nil)

	done := make(chan struct{})
	go func() {
		assert.Nil(t, queue.Dequeue(), "Expect nil dequeue")
		close(done)
	}()

	queue.Close()
	runtime.Gosched()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Close didn't unblock Dequeue.")
	}
}

func TestPriorityQueueClose(t *testing.T) {
	queue := NewPriorityQueue(nil)
	queue.Enqueue(simpleItem(1))
	queue.Close()
	assert.Panics(t, func() { queue.Enqueue(simpleItem(2)) })
	assert.Equal(t, simpleItem(1), queue.Dequeue().(simpleItem))

	queue = NewPriorityQueue(nil)
	queue.Enqueue(simpleItem(1))
	queue.DiscardAndClose()
	assert.Panics(t, func() { queue.Enqueue(simpleItem(2)) })
	assert.Nil(t, queue.Dequeue())
}

type orderedItem struct {
	priority int64
	id       int
}

func (i orderedItem) Priority() int64 {
	return i.priority
}

func TestPriorityQueueFIFOWithinSamePriority(t *testing.T) {
	queue := NewPriorityQueue(nil)
	for id := 1; id <= 6; id++ {
		queue.Enqueue(orderedItem{priority: 1, id: id})
	}

	for id := 1; id <= 6; id++ {
		assert.Equal(t, id, queue.Dequeue().(orderedItem).id, "Expected items with the same priority in arrival order")
	}
}

func TestPriorityQueueFIFOWithinPriorityLevels(t *testing.T) {
	queue := NewPriorityQueue(nil)
	queue.Enqueue(orderedItem{priority: 1, id: 1})
	queue.Enqueue(orderedItem{priority: 2, id: 2})
	queue.Enqueue(orderedItem{priority: 1, id: 3})
	queue.Enqueue(orderedItem{priority: 2, id: 4})
	queue.Enqueue(orderedItem{priority: 1, id: 5})

	assert.Equal(t, 2, queue.Peek().(orderedItem).id)
	for _, id := range []int{2, 4, 1, 3, 5} {
		assert.Equal(t, id, queue.Dequeue().(orderedItem).id)
	}
}

func TestPriorityQueueNoStarvationUnderLoad(t *testing.T) {
	queue := NewPriorityQueue(nil)
	next := 1
	for ; next <= 4; next++ {
		queue.Enqueue(orderedItem{priority: 1, id: next})
	}

	// Serve one query and receive a new one, repeatedly. Every query that was
	// waiting at the start must be served before any query that arrived later.
	for served := 1; served <= 4; served++ {
		assert.Equal(t, served, queue.Dequeue().(orderedItem).id)
		queue.Enqueue(orderedItem{priority: 1, id: next})
		next++
	}
}
