package util

import (
	"container/heap"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
)

const (
	maxQueueLength = 100000000
)

// PriorityQueue is a priority queue.
type PriorityQueue struct {
	lock   sync.Mutex
	ch     chan bool
	closed bool
	hit    map[string]int
	queue  queue
}

// Op is an operation on the priority queue.
type Op interface {
	Key() string
	Priority() int64 // The larger the number the higher the priority.
}

// item is an item in the queue. It's an Op + an index.
type item struct {
	index   int
	payload Op
}

type queue []*item

func (q queue) Len() int           { return len(q) }
func (q queue) Less(i, j int) bool { return q[i].payload.Priority() > q[j].payload.Priority() }
func (q queue) Top() interface{}   { return q[0] }

func (q queue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index = i
	q[j].index = j
}

// Push and Pop use pointer receivers because they modify the slice's length,
// not just its contents.
func (q *queue) Push(x interface{}) {
	n := q.Len()
	y := x.(*item)
	y.index = n
	*q = append(*q, y)
}

func (q *queue) Pop() interface{} {
	old := *q
	n := len(old)
	x := old[n-1]
	x.index = -1
	*q = old[0 : n-1]
	return x
}

// NewPriorityQueue makes a new priority queue.
func NewPriorityQueue() *PriorityQueue {
	pq := &PriorityQueue{
		hit: map[string]int{},
		ch:  make(chan bool, maxQueueLength),
	}
	heap.Init(&pq.queue)
	return pq
}

// Length returns the length of the queue.
func (pq *PriorityQueue) Length() int {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	return len(pq.queue)
}

// Close signals that the queue is closed. A closed queue will not accept new
// items.
func (pq *PriorityQueue) Close() {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	pq.closed = true
	close(pq.ch)
}

// enqueue adds an operation to the queue in priority order, but does *not*
// make it available for dequeueing. If the operation is already on the queue,
// it will be ignored.
//
// Return 'true' if the item is newly added to the queue, 'false' if it was
// already there.
func (pq *PriorityQueue) enqueue(op Op) {
	if pq.closed {
		panic("enqueue on closed queue")
	}

	key := op.Key()
	index, enqueued := pq.hit[key]
	if enqueued {
		item := pq.queue[index]
		item.payload = op
		heap.Fix(&pq.queue, index)
		pq.hit[key] = item.index
	} else {
		item := item{-1, op}
		heap.Push(&pq.queue, &item)
		pq.hit[key] = item.index
	}
}

// Enqueue adds an operation to the queue in priority order. If the operation
// is already on the queue, it will be ignored.
func (pq *PriorityQueue) Enqueue(op Op) {
	pq.lock.Lock()
	pq.enqueue(op)
	pq.lock.Unlock()
	pq.ch <- true
}

// Dequeue will return the op with the highest priority; block if queue is
// empty; returns nil if queue is closed.
func (pq *PriorityQueue) Dequeue() Op {
	for {
		select {
		case <-pq.ch:
			pq.lock.Lock()
			defer pq.lock.Unlock()

			if len(pq.queue) == 0 {
				if pq.closed {
					return nil
				}
				continue
			}
			item := heap.Pop(&pq.queue).(*item)
			delete(pq.hit, item.payload.Key())
			return item.payload
		}
	}
}

// DelayedCall is a function that we're not going to run yet.
type DelayedCall struct {
	clock      clockwork.Clock
	ch         chan bool
	elapsed    <-chan time.Time
	cancelled  chan struct{}
	terminated chan struct{}
}

// ScheduledItem is an item in a queue of scheduled items.
type ScheduledItem interface {
	Key() string
	// Scheduled returns the earliest possible time the time is available for
	// dequeueing.
	Scheduled() time.Time
}

// scheduledOp adapts a ScheduledItem to an Op
type scheduledOp struct {
	ScheduledItem
}

// Priority implements Op.
func (op scheduledOp) Priority() int64 {
	return -op.Scheduled().Unix()
}

// SchedulingQueue is like a priority queue, but the first item is the oldest
// scheduled item.
type SchedulingQueue struct {
	*PriorityQueue
	clock clockwork.Clock
	timer *DelayedCall
}

// NewSchedulingQueue makes a new priority queue.
func NewSchedulingQueue(clock clockwork.Clock) *SchedulingQueue {
	pq := NewPriorityQueue()
	return &SchedulingQueue{
		PriorityQueue: pq,
		clock:         clock,
	}
}

func (sq *SchedulingQueue) front() ScheduledItem {
	if len(sq.queue) == 0 {
		return nil
	}
	top := sq.PriorityQueue.queue.Top().(*item)
	return top.payload.(scheduledOp).ScheduledItem
}

// Enqueue schedules an item for later Dequeueing.
func (sq *SchedulingQueue) Enqueue(item ScheduledItem) {
	sq.lock.Lock()
	sq.enqueue(scheduledOp{item})
	sq.lock.Unlock()
	sq.ch <- true
}

// Dequeue takes an item from the queue. If there are no items, or the first
// item isn't ready to be scheduled, it blocks.
func (sq *SchedulingQueue) Dequeue() ScheduledItem {
	// Wait until there's something to dequeue.
	var delayer <-chan time.Time
	for {
		select {
		case <-sq.ch:
			sq.lock.Lock()
			front := sq.front()
			if front == nil {
				sq.lock.Unlock()
				if sq.closed {
					// Queue is empty and can't have anything more added, so
					// no point waiting.
					return nil
				}
				continue
			}

			delay := front.Scheduled().Sub(sq.clock.Now())
			if delay > 0 {
				delayer = sq.clock.After(delay)
				sq.lock.Unlock()
				continue
			}
			item := heap.Pop(&sq.queue).(*item)
			delete(sq.hit, item.payload.Key())
			sq.lock.Unlock()
			return item.payload.(scheduledOp).ScheduledItem
		case <-delayer:
			sq.ch <- true
		}
	}
}
