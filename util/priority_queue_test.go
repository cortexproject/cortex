package util

import (
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
)

type simpleItem int64

func (i simpleItem) Priority() int64 {
	return int64(i)
}

func (i simpleItem) Key() string {
	return strconv.FormatInt(int64(i), 10)
}

type richItem struct {
	priority int64
	key      string
	value    string
}

func (r richItem) Priority() int64 {
	return r.priority
}

func (r richItem) Key() string {
	return r.key
}

func TestPriorityQueueBasic(t *testing.T) {
	queue := NewPriorityQueue()
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
	queue := NewPriorityQueue()
	queue.Enqueue(simpleItem(1))
	queue.Enqueue(simpleItem(2))

	assert.Equal(t, simpleItem(2), queue.Dequeue().(simpleItem), "Expected to dequeue simpleItem(2)")
	assert.Equal(t, simpleItem(1), queue.Dequeue().(simpleItem), "Expected to dequeue simpleItem(1)")

	queue.Close()
	assert.Nil(t, queue.Dequeue(), "Expect nil dequeue")
}

func TestPriorityQueuePriorities2(t *testing.T) {
	queue := NewPriorityQueue()
	queue.Enqueue(simpleItem(2))
	queue.Enqueue(simpleItem(1))

	assert.Equal(t, simpleItem(2), queue.Dequeue().(simpleItem), "Expected to dequeue simpleItem(2)")
	assert.Equal(t, simpleItem(1), queue.Dequeue().(simpleItem), "Expected to dequeue simpleItem(1)")

	queue.Close()
	assert.Nil(t, queue.Dequeue(), "Expect nil dequeue")
}

func TestPriorityQueueDedupe(t *testing.T) {
	queue := NewPriorityQueue()
	queue.Enqueue(simpleItem(1))
	queue.Enqueue(simpleItem(1))

	assert.Equal(t, 1, queue.Length(), "Expected length = 1")
	assert.Equal(t, simpleItem(1), queue.Dequeue().(simpleItem), "Expected to dequeue simpleItem(1)")

	queue.Close()
	assert.Nil(t, queue.Dequeue(), "Expect nil dequeue")
}

// If we enqueue a second item with the same key as an existing item, the new
// item replaces the old, adjusting priority if necessary.
func TestPriorityQueueRichDedupe(t *testing.T) {
	queue := NewPriorityQueue()
	bar := richItem{2, "bar", "middling priority"}
	foo1 := richItem{1, "foo", "less important than bar"}
	foo2 := richItem{3, "foo", "more important than bar"}
	queue.Enqueue(bar)
	queue.Enqueue(foo1)
	queue.Enqueue(foo2)
	queue.Close()

	assert.Equal(t, 2, queue.Length(), "Expected length = 2")
	assert.Equal(t, foo2, queue.Dequeue().(richItem), fmt.Sprintf("Expected to dequeue %v", foo2))
	assert.Equal(t, bar, queue.Dequeue().(richItem), fmt.Sprintf("Expected to dequeue %v", bar))
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

// assertDequeues asserts that queue.Dequeue() is simpleItem.
func assertDequeues(t *testing.T, item sched, queue *SchedulingQueue) {
	assert.Equal(t, item, queue.Dequeue().(sched), fmt.Sprintf("Expected to dequeue %v", item))
}

func TestSchedulingQueuePriorities(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queue := NewSchedulingQueue(clock)
	oneHourAgo := sched(clock.Now().Add(-time.Hour))
	twoHoursAgo := sched(clock.Now().Add(-2 * time.Hour))
	queue.Enqueue(oneHourAgo)
	queue.Enqueue(twoHoursAgo)

	assert.Equal(t, twoHoursAgo, queue.front().(sched))
	assertDequeues(t, twoHoursAgo, queue)
	assertDequeues(t, oneHourAgo, queue)

	queue.Close()
	assert.Nil(t, queue.Dequeue(), "Expect nil dequeue")
}

func TestSchedulingQueuePriorities2(t *testing.T) {
	clock := clockwork.NewRealClock()
	queue := NewSchedulingQueue(clock)
	oneHourAgo := sched(clock.Now().Add(-time.Hour))
	twoHoursAgo := sched(clock.Now().Add(-2 * time.Hour))
	queue.Enqueue(twoHoursAgo)
	queue.Enqueue(oneHourAgo)

	assertDequeues(t, twoHoursAgo, queue)
	assertDequeues(t, oneHourAgo, queue)

	queue.Close()
	assert.Nil(t, queue.Dequeue(), "Expect nil dequeue")
}

func TestSchedulingQueueWaitOnEmpty(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queue := NewSchedulingQueue(clock)

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
	clock := clockwork.NewFakeClock()
	queue := NewSchedulingQueue(clock)

	oneHourAgo := sched(clock.Now().Add(-time.Hour))
	done := make(chan struct{})
	go func() {
		assertDequeues(t, oneHourAgo, queue)
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

func TestSchedulingQueueBlockingBug(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queue := NewSchedulingQueue(clock)
	twoHoursAgo := sched(clock.Now().Add(-2 * time.Hour))
	queue.Enqueue(twoHoursAgo)
	assertDequeues(t, twoHoursAgo, queue)

	done := make(chan struct{})
	delay := 1 * time.Hour
	soon := sched(clock.Now().Add(delay))
	go func() {
		assertDequeues(t, soon, queue)
		close(done)
	}()

	queue.Enqueue(soon)
	clock.BlockUntil(1)
	clock.Advance(2 * delay)
	<-done
}

func TestSchedulingQueueRescheduling(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queue := NewSchedulingQueue(clock)

	oneHourAgo := sched(clock.Now().Add(-1 * time.Hour))
	oneHourFromNow := sched(clock.Now().Add(1 * time.Hour))
	queue.Enqueue(oneHourFromNow)
	clock.BlockUntil(1)

	done := make(chan struct{})
	go func() {
		assertDequeues(t, oneHourAgo, queue)
		close(done)
	}()

	queue.Enqueue(oneHourAgo)
	<-done
}

func TestDelayedCall(t *testing.T) {
	clock := clockwork.NewFakeClock()
	var wg sync.WaitGroup
	var called uint64

	wg.Add(1)
	DelayCall(clock, 5*time.Hour, func() {
		atomic.AddUint64(&called, 1)
		wg.Done()
	})

	clock.BlockUntil(1)
	assert.Equal(t, uint64(0), atomic.LoadUint64(&called))
	clock.Advance(2 * time.Hour)
	assert.Equal(t, uint64(0), atomic.LoadUint64(&called))
	clock.Advance(2 * time.Hour)
	assert.Equal(t, uint64(0), atomic.LoadUint64(&called))
	clock.Advance(2 * time.Hour)
	wg.Wait()
	assert.Equal(t, uint64(1), atomic.LoadUint64(&called))
}
