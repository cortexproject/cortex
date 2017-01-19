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
