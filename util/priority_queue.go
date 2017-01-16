package util

import (
	"container/heap"
	"sync"
	"time"
)

// PriorityQueue is a priority queue.
type PriorityQueue struct {
	lock   sync.Mutex
	cond   *sync.Cond
	closed bool
	hit    map[string]struct{}
	queue  queue
}

// Op is an operation on the priority queue.
type Op interface {
	Key() string
	Priority() int64 // The larger the number the higher the priority.
}

type queue []Op

func (q queue) Len() int           { return len(q) }
func (q queue) Less(i, j int) bool { return q[i].Priority() > q[j].Priority() }
func (q queue) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }
func (q queue) Top() interface{}   { return q[0] }

// Push and Pop use pointer receivers because they modify the slice's length,
// not just its contents.
func (q *queue) Push(x interface{}) {
	*q = append(*q, x.(Op))
}

func (q *queue) Pop() interface{} {
	old := *q
	n := len(old)
	x := old[n-1]
	*q = old[0 : n-1]
	return x
}

// NewPriorityQueue makes a new priority queue.
func NewPriorityQueue() *PriorityQueue {
	pq := &PriorityQueue{
		hit: map[string]struct{}{},
	}
	pq.cond = sync.NewCond(&pq.lock)
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
	pq.cond.Broadcast()
}

// enqueue adds an operation to the queue in priority order, but does *not*
// make it available for dequeueing. If the operation is already on the queue,
// it will be ignored.
//
// Return 'true' if the item is newly added to the queue, 'false' if it was
// already there.
func (pq *PriorityQueue) enqueue(op Op) bool {
	if pq.closed {
		panic("enqueue on closed queue")
	}

	_, enqueued := pq.hit[op.Key()]
	if enqueued {
		return false
	}

	pq.hit[op.Key()] = struct{}{}
	heap.Push(&pq.queue, op)
	return true
}

// Enqueue adds an operation to the queue in priority order. If the operation
// is already on the queue, it will be ignored.
func (pq *PriorityQueue) Enqueue(op Op) {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	if pq.enqueue(op) {
		pq.cond.Broadcast()
	}
}

// Dequeue will return the op with the highest priority; block if queue is
// empty; returns nil if queue is closed.
func (pq *PriorityQueue) Dequeue() Op {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	for len(pq.queue) == 0 && !pq.closed {
		pq.cond.Wait()
	}

	if len(pq.queue) == 0 && pq.closed {
		return nil
	}

	op := heap.Pop(&pq.queue).(Op)
	delete(pq.hit, op.Key())
	return op
}

// ScheduledItem is an item in a queue of scheduled items.
type ScheduledItem interface {
	Key() string
	// Scheduled returns the earliest possible time the time is available for
	// dequeueing.
	Scheduled() time.Time
}

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
	timer *time.Timer
}

// NewSchedulingQueue makes a new priority queue.
func NewSchedulingQueue() *SchedulingQueue {
	pq := NewPriorityQueue()
	return &SchedulingQueue{
		PriorityQueue: pq,
	}
}

func (sq *SchedulingQueue) front() ScheduledItem {
	if len(sq.queue) == 0 {
		return nil
	}
	top := sq.PriorityQueue.queue.Top().(scheduledOp)
	return top.ScheduledItem
}

func (sq *SchedulingQueue) frontChanged() {
	front := sq.front()
	if front == nil {
		sq.cancelTimer()
		return
	}

	delay := front.Scheduled().Sub(time.Now())
	if delay <= 0 {
		sq.cancelTimer()
		sq.cond.Broadcast()
		return
	}

	sq.rescheduleTimer(delay)
}

func (sq *SchedulingQueue) cancelTimer() {
	if sq.timer != nil {
		// Note: the timer might have fired by this point, but that's probably
		// OK because the only thing we're waiting for is a broadcast to
		// `cond`, which wakes up the Dequeue loop.
		sq.timer.Stop()
		sq.timer = nil
	}
}

func (sq *SchedulingQueue) rescheduleTimer(delay time.Duration) {
	if sq.timer == nil {
		sq.timer = time.AfterFunc(delay, sq.cond.Broadcast)
	} else {
		// Note: timer might have fired by this point, but that's OK. See note
		// in `cancelTimer` for reasons why.
		sq.timer.Stop()
		sq.timer.Reset(delay)
	}
}

// Enqueue schedules an item for later Dequeueing.
func (sq *SchedulingQueue) Enqueue(item ScheduledItem) {
	sq.lock.Lock()
	defer sq.lock.Unlock()

	if !sq.enqueue(scheduledOp{item}) {
		return
	}
	front := sq.front() // Won't be nil because we just added something!
	if front.Key() == item.Key() {
		// New item went to front of the queue.
		sq.frontChanged()
	}
}

// Dequeue takes an item from the queue. If there are no items, or the first
// item isn't ready to be scheduled, it blocks.
func (sq *SchedulingQueue) Dequeue() ScheduledItem {
	sq.lock.Lock()
	defer sq.lock.Unlock()

	// Wait until there's something to dequeue.
	for {
		front := sq.front()
		if front == nil && sq.closed {
			// Queue is empty and can't have anything more added, so no point
			// waiting.
			return nil
		}
		if front != nil {
			if !front.Scheduled().After(time.Now()) {
				// Front item is ready to be run, so no point waiting.
				break
			}
		}
		// Either the queue is empty & open, or the first item is scheduled
		// for some time in the future. Wait for that to change.
		sq.cond.Wait()
	}

	op := heap.Pop(&sq.queue).(scheduledOp)
	delete(sq.hit, op.Key())
	sq.frontChanged()
	return op.ScheduledItem
}
