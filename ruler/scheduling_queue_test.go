package ruler

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
)

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

	queue.Close()
	runtime.Gosched()
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

	queue.Enqueue(oneHourAgo)
	runtime.Gosched()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Queueing item didn't unblock Dequeue.")
	}
}

// We can dequeue all the items after closing the queue.
func TestSchedulingQueueEmptiesAfterClosing(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queue := NewSchedulingQueue(clock)

	twoHoursAgo := sched(clock.Now().Add(-2 * time.Hour))
	oneHourAgo := sched(clock.Now().Add(-1 * time.Hour))

	queue.Enqueue(twoHoursAgo)
	queue.Enqueue(oneHourAgo)
	queue.Close()

	assertDequeues(t, twoHoursAgo, queue)
	assertDequeues(t, oneHourAgo, queue)
	assert.Nil(t, queue.Dequeue(), "Expect nil dequeue")
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
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Advancing the clock didn't trigger dequeue.")
	}
}

func TestSchedulingQueueRescheduling(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queue := NewSchedulingQueue(clock)

	oneHourAgo := sched(clock.Now().Add(-1 * time.Hour))
	oneHourFromNow := sched(clock.Now().Add(1 * time.Hour))
	queue.Enqueue(oneHourFromNow)

	done := make(chan struct{})
	go func() {
		assertDequeues(t, oneHourAgo, queue)
		close(done)
	}()

	queue.Enqueue(oneHourAgo)
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Dequeue never happens.")
	}
}

func TestSchedulingQueueCloseWhileWaiting(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queue := NewSchedulingQueue(clock)

	oneHourFromNow := sched(clock.Now().Add(1 * time.Hour))
	queue.Enqueue(oneHourFromNow)

	clock.BlockUntil(1)
	clock.Advance(30 * time.Minute)
	queue.Close()

	clock.Advance(1 * time.Hour)
	assertDequeues(t, oneHourFromNow, queue)
	assert.Nil(t, queue.Dequeue(), "Expect nil dequeue")
}

type richItem struct {
	scheduled time.Time
	key       string
	payload   string
}

func (r richItem) Scheduled() time.Time {
	return r.scheduled
}

func (r richItem) Key() string {
	return r.key
}

// If we enqueue a second item with the same key as an existing item, the new
// item replaces the old, adjusting priority if necessary.
func TestSchedulingQueueDedupe(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queue := NewSchedulingQueue(clock)

	threeHoursAgo := clock.Now().Add(-3 * time.Hour)
	twoHoursAgo := clock.Now().Add(-2 * time.Hour)
	oneHourAgo := clock.Now().Add(-1 * time.Hour)

	bar := richItem{twoHoursAgo, "bar", "middling priority"}
	foo1 := richItem{oneHourAgo, "foo", "less important than bar"}
	foo2 := richItem{threeHoursAgo, "foo", "more important than bar"}
	queue.Enqueue(bar)
	queue.Enqueue(foo1)
	queue.Enqueue(foo2)
	queue.Close()

	assert.Equal(t, foo2, queue.Dequeue().(richItem), fmt.Sprintf("Expected to dequeue %v", foo2))
	assert.Equal(t, bar, queue.Dequeue().(richItem), fmt.Sprintf("Expected to dequeue %v", bar))
	assert.Nil(t, queue.Dequeue(), "Expect nil dequeue")
}

func TestSchedulingQueueDedupe2(t *testing.T) {
	clock := clockwork.NewFakeClock()
	queue := NewSchedulingQueue(clock)

	threeHoursAgo := clock.Now().Add(-3 * time.Hour)
	twoHoursAgo := clock.Now().Add(-2 * time.Hour)
	oneHourAgo := clock.Now().Add(-1 * time.Hour)

	bar := richItem{twoHoursAgo, "bar", "middling priority"}
	foo1 := richItem{oneHourAgo, "foo", "less important than bar"}
	foo2 := richItem{threeHoursAgo, "foo", "more important than bar"}
	queue.Enqueue(bar)
	queue.Enqueue(foo2)

	assert.Equal(t, foo2, queue.Dequeue().(richItem), fmt.Sprintf("Expected to dequeue %v", foo2))

	queue.Enqueue(foo1)
	queue.Close()

	assert.Equal(t, bar, queue.Dequeue().(richItem), fmt.Sprintf("Expected to dequeue %v", bar))
	assert.Equal(t, foo1, queue.Dequeue().(richItem), fmt.Sprintf("Expected to dequeue %v", foo2))
	assert.Nil(t, queue.Dequeue(), "Expect nil dequeue")
}
