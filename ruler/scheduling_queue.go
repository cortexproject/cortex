package ruler

import (
	"container/heap"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	queueLength = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "rules_queue_length",
		Help:      "The length of the rules queue.",
	})
)

func init() {
	prometheus.MustRegister(queueLength)
}

// ScheduledItem is an item in a queue of scheduled items.
type ScheduledItem interface {
	Key() string
	// Scheduled returns the earliest possible time the time is available for
	// dequeueing.
	Scheduled() time.Time
}

type item struct {
	index   *int
	payload ScheduledItem
}

type scheduledItems []*item

func (q scheduledItems) Len() int { return len(q) }
func (q scheduledItems) Less(i, j int) bool {
	return q[i].payload.Scheduled().Before(q[j].payload.Scheduled())
}
func (q scheduledItems) Top() interface{} { return q[0] }

func (q scheduledItems) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

// Push and Pop use pointer receivers because they modify the slice's length,
// not just its contents.
func (q *scheduledItems) Push(x interface{}) {
	n := len(*q)
	y := x.(*item)
	*y.index = n
	*q = append(*q, y)
}

func (q *scheduledItems) Pop() interface{} {
	old := *q
	n := len(old)
	x := old[n-1]
	*x.index = -1
	*q = old[0 : n-1]
	return x
}

type queueState struct {
	items scheduledItems
	hit   map[string]*int
}

func (q *queueState) Enqueue(op ScheduledItem) {
	key := op.Key()
	i, enqueued := q.hit[key]
	if enqueued {
		item := q.items[*i]
		item.payload = op
		heap.Fix(&q.items, *i)
	} else {
		var index int
		q.hit[key] = &index
		item := item{&index, op}
		heap.Push(&q.items, &item)
	}
	queueLength.Set(float64(len(q.items)))
}

func (q *queueState) Dequeue() ScheduledItem {
	item := heap.Pop(&q.items).(*item)
	delete(q.hit, item.payload.Key())
	queueLength.Set(float64(len(q.items)))
	return item.payload
}

func (q *queueState) Front() ScheduledItem {
	return q.items.Top().(*item).payload
}

func (q *queueState) Length() int {
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
		items: scheduledItems{},
		hit:   map[string]*int{},
	}

	for {
		// Nothing on the queue?  Wait for something to be added.
		if q.Length() == 0 {
			next, ok := <-sq.add

			// Iff sq.add is closed (and there is nothing on the queue),
			// we can close sq.next and stop this goroutine
			if !ok {
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
			case justAdded, ok := <-sq.add:
				if ok {
					q.Enqueue(justAdded)
				}
			}
			continue
		}

		// Item on the queue that needs waiting for?
		// Wait on a timer _or_ for something to be added.
		select {
		case <-sq.clock.After(delay):
		case justAdded, ok := <-sq.add:
			if ok {
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
