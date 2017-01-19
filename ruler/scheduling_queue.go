package ruler

import (
	"container/heap"
	"time"

	"github.com/jonboulle/clockwork"
)

// ScheduledItem is an item in a queue of scheduled items.
type ScheduledItem interface {
	Key() string
	// Scheduled returns the earliest possible time the time is available for
	// dequeueing.
	Scheduled() time.Time
}

type scheduledItems []ScheduledItem

func (q scheduledItems) Len() int           { return len(q) }
func (q scheduledItems) Less(i, j int) bool { return q[i].Scheduled().Before(q[j].Scheduled()) }
func (q scheduledItems) Top() interface{}   { return q[0] }

func (q scheduledItems) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

// Push and Pop use pointer receivers because they modify the slice's length,
// not just its contents.
func (q *scheduledItems) Push(x interface{}) {
	y := x.(ScheduledItem)
	*q = append(*q, y)
}

func (q *scheduledItems) Pop() interface{} {
	old := *q
	n := len(old)
	x := old[n-1]
	*q = old[0 : n-1]
	return x
}

// SchedulingQueue is like a priority queue, but the first item is the oldest
// scheduled item.
type SchedulingQueue struct {
	clock     clockwork.Clock
	add, next chan ScheduledItem
}

// NewSchedulingQueue makes a new priority queue.
func NewSchedulingQueue(clock clockwork.Clock) *SchedulingQueue {
	sq := &SchedulingQueue{
		clock: clock,
		add:   make(chan ScheduledItem),
		next:  make(chan ScheduledItem),
	}
	go sq.run()
	return sq
}

func (sq *SchedulingQueue) Close() {
	close(sq.add)
}

func (sq *SchedulingQueue) run() {
	items := scheduledItems{}
	for {
		// Nothing on the queue?  Wait for something to be added.
		if len(items) == 0 {
			next, ok := <-sq.add

			// Iff sq.add is closed (and there is nothing on the queue),
			// we can close sq.next and stop this goroutine
			if !ok {
				close(sq.next)
				return
			}

			heap.Push(&items, next)
			continue
		}

		next := items.Top().(ScheduledItem)
		delay := next.Scheduled().Sub(sq.clock.Now())

		// Item on the queue that is ready now?
		if delay <= 0 {
			select {
			case sq.next <- next:
				heap.Pop(&items)
			case item := <-sq.add:
				heap.Push(&items, item)
			}
			continue
		}

		// Item on the queue that needs waiting for?
		// Wait on a timer _or_ for something to be added.
		select {
		case <-sq.clock.After(delay):
		case item := <-sq.add:
			heap.Push(&items, item)
		}
	}
}

// Enqueue schedules an item for later Dequeueing.
func (sq *SchedulingQueue) Enqueue(item ScheduledItem) {
	sq.add <- item
}

// Dequeue takes an item from the queue.
// If there are no items, or the first item isn't ready to be scheduled, it
// blocks. If there queue is closed, this will return nil.
func (sq *SchedulingQueue) Dequeue() ScheduledItem {
	return <-sq.next
}
