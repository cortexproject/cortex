package ruler

import (
	"container/heap"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	itemEvaluationLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "scheduling_queue_item_latency",
		Help:      "Difference between the items scheduled evaluation time and actual evaluation time",
		Buckets:   prometheus.DefBuckets,
	})
)

// ScheduledItem is an item in a queue of scheduled items.
type ScheduledItem interface {
	Key() string
	// Scheduled returns the earliest possible time the time is available for
	// dequeueing.
	Scheduled() time.Time
}

type queueState struct {
	items []ScheduledItem
	hit   map[string]int
}

// Less implements heap.Interface
func (q queueState) Less(i, j int) bool {
	return q.items[i].Scheduled().Before(q.items[j].Scheduled())
}

// Pop implements heap.Interface
func (q *queueState) Pop() interface{} {
	old := q.items
	n := len(old)
	x := old[n-1]
	delete(q.hit, x.Key())
	q.items = old[0 : n-1]
	return x
}

// Push implements heap.Interface
func (q *queueState) Push(x interface{}) {
	n := len(q.items)
	y := x.(ScheduledItem)
	q.hit[y.Key()] = n
	q.items = append(q.items, y)
}

func (q *queueState) Swap(i, j int) {
	q.hit[q.items[i].Key()] = j
	q.hit[q.items[j].Key()] = i
	q.items[i], q.items[j] = q.items[j], q.items[i]
}

func (q *queueState) Enqueue(op ScheduledItem) {
	key := op.Key()
	i, enqueued := q.hit[key]
	if enqueued {
		q.items[i] = op
		heap.Fix(q, i)
	} else {
		heap.Push(q, op)
	}
}

func (q *queueState) Dequeue() ScheduledItem {
	item := heap.Pop(q).(ScheduledItem)
	itemEvaluationLatency.Observe(time.Now().Sub(item.Scheduled()).Seconds())
	return item
}

func (q *queueState) Front() ScheduledItem {
	return q.items[0]
}

func (q *queueState) Len() int {
	return len(q.items)
}

// SchedulingQueue is like a priority queue, but the first item is the oldest
// scheduled item. Items are only able to be dequeued after the time they are
// scheduled to be run.
type SchedulingQueue struct {
	clock     clockwork.Clock
	add, next chan ScheduledItem
}

// NewSchedulingQueue makes a new scheduling queue.
func NewSchedulingQueue(clock clockwork.Clock) *SchedulingQueue {
	sq := &SchedulingQueue{
		clock: clock,
		add:   make(chan ScheduledItem),
		next:  make(chan ScheduledItem),
	}
	go sq.run()
	return sq
}

// Close the scheduling queue. No more items can be added. Items can be
// dequeued until there are none left.
func (sq *SchedulingQueue) Close() {
	close(sq.add)
}

func (sq *SchedulingQueue) run() {
	q := queueState{
		items: []ScheduledItem{},
		hit:   map[string]int{},
	}

	for {
		// Nothing on the queue?  Wait for something to be added.
		if q.Len() == 0 {
			next, open := <-sq.add

			// If sq.add is closed (and there is nothing on the queue),
			// we can close sq.next and stop this goroutine
			if !open {
				close(sq.next)
				return
			}

			q.Enqueue(next)
			continue
		}

		next := q.Front()
		delay := next.Scheduled().Sub(sq.clock.Now())

		// Item on the queue that is ready now?
		if delay <= 0 {
			select {
			case sq.next <- next:
				q.Dequeue()
			case justAdded, open := <-sq.add:
				if open {
					q.Enqueue(justAdded)
				}
			}
			continue
		}

		// Item on the queue that needs waiting for?
		// Wait on a timer _or_ for something to be added.
		select {
		case <-sq.clock.After(delay):
		case justAdded, open := <-sq.add:
			if open {
				q.Enqueue(justAdded)
			}
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
