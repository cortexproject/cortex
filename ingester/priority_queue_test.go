package ingester

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type item int64

func (i item) Priority() int64 {
	return int64(i)
}

func (i item) Key() string {
	return strconv.FormatInt(int64(i), 10)
}

func TestPriorityQueueBasic(t *testing.T) {
	queue := newPriorityQueue()
	assert.Equal(t, 0, queue.Length(), "Expected length = 0")

	queue.Enqueue(item(1))
	assert.Equal(t, 1, queue.Length(), "Expected length = 1")

	i, ok := queue.Dequeue().(item)
	assert.True(t, ok, "Expected cast to succeed")
	assert.Equal(t, item(1), i, "Expected to dequeue item(1)")

	queue.Close()
	assert.Nil(t, queue.Dequeue(), "Expect nil dequeue")
}

func TestPriorityQueuePriorities(t *testing.T) {
	queue := newPriorityQueue()
	queue.Enqueue(item(1))
	queue.Enqueue(item(2))

	assert.Equal(t, item(2), queue.Dequeue().(item), "Expected to dequeue item(2)")
	assert.Equal(t, item(1), queue.Dequeue().(item), "Expected to dequeue item(1)")

	queue.Close()
	assert.Nil(t, queue.Dequeue(), "Expect nil dequeue")
}

func TestPriorityQueuePriorities2(t *testing.T) {
	queue := newPriorityQueue()
	queue.Enqueue(item(2))
	queue.Enqueue(item(1))

	assert.Equal(t, item(2), queue.Dequeue().(item), "Expected to dequeue item(2)")
	assert.Equal(t, item(1), queue.Dequeue().(item), "Expected to dequeue item(1)")

	queue.Close()
	assert.Nil(t, queue.Dequeue(), "Expect nil dequeue")
}

func TestPriorityQueueDedupe(t *testing.T) {
	queue := newPriorityQueue()
	queue.Enqueue(item(1))
	queue.Enqueue(item(1))

	assert.Equal(t, 1, queue.Length(), "Expected length = 1")
	assert.Equal(t, item(1), queue.Dequeue().(item), "Expected to dequeue item(1)")

	queue.Close()
	assert.Nil(t, queue.Dequeue(), "Expect nil dequeue")
}

func TestPriorityQueueWait(t *testing.T) {
	queue := newPriorityQueue()

	done := make(chan struct{})
	go func() {
		assert.Nil(t, queue.Dequeue(), "Expect nil dequeue")
		close(done)
	}()

	queue.Close()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Close didn't unblock Dequeue.")
	}
}
