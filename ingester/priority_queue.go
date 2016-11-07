package ingester

import (
	"container/heap"
	"sync"
)

type priorityQueue struct {
	lock   sync.Mutex
	cond   *sync.Cond
	closed bool
	hit    map[string]struct{}
	queue  queue
}

type op interface {
	Key() string
	Priority() int64
}

type queue []op

func (q queue) Len() int           { return len(q) }
func (q queue) Less(i, j int) bool { return q[i].Priority() < q[j].Priority() }
func (q queue) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }

// Push and Pop use pointer receivers because they modify the slice's length,
// not just its contents.
func (q *queue) Push(x interface{}) {
	*q = append(*q, x.(op))
}

func (q *queue) Pop() interface{} {
	old := *q
	n := len(old)
	x := old[n-1]
	*q = old[0 : n-1]
	return x
}

func newPriorityQueue() *priorityQueue {
	pq := &priorityQueue{
		hit: map[string]struct{}{},
	}
	pq.cond = sync.NewCond(&pq.lock)
	heap.Init(&pq.queue)
	return pq
}

func (pq *priorityQueue) Close() {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	pq.closed = true
	pq.cond.Broadcast()
}

func (pq *priorityQueue) Enqueue(op op) {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	if pq.closed {
		panic("enqueue on closed queue")
	}

	_, enqueued := pq.hit[op.Key()]
	if enqueued {
		return
	}

	heap.Push(&pq.queue, op)
	pq.cond.Broadcast()
}

func (pq *priorityQueue) Dequeue() op {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	for len(pq.queue) == 0 && !pq.closed {
		pq.cond.Wait()
	}

	if len(pq.queue) == 0 && pq.closed {
		return nil
	}

	op := heap.Pop(&pq.queue).(op)
	delete(pq.hit, op.Key())
	return op
}
