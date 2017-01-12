package util

import (
	"fmt"
	"runtime"
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
	queue := NewPriorityQueue()
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
	queue := NewPriorityQueue()
	queue.Enqueue(item(1))
	queue.Enqueue(item(2))

	assert.Equal(t, item(2), queue.Dequeue().(item), "Expected to dequeue item(2)")
	assert.Equal(t, item(1), queue.Dequeue().(item), "Expected to dequeue item(1)")

	queue.Close()
	assert.Nil(t, queue.Dequeue(), "Expect nil dequeue")
}

func TestPriorityQueuePriorities2(t *testing.T) {
	queue := NewPriorityQueue()
	queue.Enqueue(item(2))
	queue.Enqueue(item(1))

	assert.Equal(t, item(2), queue.Dequeue().(item), "Expected to dequeue item(2)")
	assert.Equal(t, item(1), queue.Dequeue().(item), "Expected to dequeue item(1)")

	queue.Close()
	assert.Nil(t, queue.Dequeue(), "Expect nil dequeue")
}

func TestPriorityQueueDedupe(t *testing.T) {
	queue := NewPriorityQueue()
	queue.Enqueue(item(1))
	queue.Enqueue(item(1))

	assert.Equal(t, 1, queue.Length(), "Expected length = 1")
	assert.Equal(t, item(1), queue.Dequeue().(item), "Expected to dequeue item(1)")

	queue.Close()
	assert.Nil(t, queue.Dequeue(), "Expect nil dequeue")
}

func TestPriorityQueueWait(t *testing.T) {
	queue := NewPriorityQueue()

	done := make(chan struct{})
	go func() {
		assert.Nil(t, queue.Dequeue(), "Expect nil dequeue")
		close(done)
	}()

	runtime.Gosched()
	queue.Close()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Close didn't unblock Dequeue.")
	}
}

type sched time.Time

func (s sched) Scheduled() time.Time {
	return time.Time(s)
}

func (s sched) Key() string {
	return time.Time(s).Format("2006-01-02 15:04:05.000")
}

func TestSchedulingQueuePriorities(t *testing.T) {
	queue := NewSchedulingQueue()
	oneHourAgo := sched(time.Now().Add(-time.Hour))
	twoHoursAgo := sched(time.Now().Add(-2 * time.Hour))
	queue.Enqueue(oneHourAgo)
	queue.Enqueue(twoHoursAgo)

	assert.Equal(t, twoHoursAgo, queue.Dequeue().(sched), fmt.Sprintf("Expected to dequeue %v", twoHoursAgo))
	assert.Equal(t, oneHourAgo, queue.Dequeue().(sched), fmt.Sprintf("Expected to dequeue %v", oneHourAgo))

	queue.Close()
	assert.Nil(t, queue.Dequeue(), "Expect nil dequeue")
}

func TestSchedulingQueuePriorities2(t *testing.T) {
	queue := NewSchedulingQueue()
	oneHourAgo := sched(time.Now().Add(-time.Hour))
	twoHoursAgo := sched(time.Now().Add(-2 * time.Hour))
	queue.Enqueue(twoHoursAgo)
	queue.Enqueue(oneHourAgo)

	assert.Equal(t, twoHoursAgo, queue.Dequeue().(sched), fmt.Sprintf("Expected to dequeue %v", twoHoursAgo))
	assert.Equal(t, oneHourAgo, queue.Dequeue().(sched), fmt.Sprintf("Expected to dequeue %v", oneHourAgo))

	queue.Close()
	assert.Nil(t, queue.Dequeue(), "Expect nil dequeue")
}

func TestSchedulingQueueWaitOnEmpty(t *testing.T) {
	queue := NewSchedulingQueue()

	done := make(chan struct{})
	go func() {
		assert.Nil(t, queue.Dequeue(), "Expect nil dequeue")
		close(done)
	}()

	runtime.Gosched()
	queue.Close()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Close didn't unblock Dequeue.")
	}
}

func TestSchedulingQueueWaitOnItem(t *testing.T) {
	queue := NewSchedulingQueue()

	oneHourAgo := sched(time.Now().Add(-time.Hour))
	done := make(chan struct{})
	go func() {
		assert.Equal(t, oneHourAgo, queue.Dequeue(), "Expected value")
		close(done)
	}()

	runtime.Gosched()
	queue.Enqueue(oneHourAgo)
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Queueing item didn't unblock Dequeue.")
	}
}
