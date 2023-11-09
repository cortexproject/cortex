package queue

import "github.com/cortexproject/cortex/pkg/util"

type userRequestQueue interface {
	enqueueRequest(Request)
	dequeueRequest(minPriority int64, checkMinPriority bool) Request
	length() int
	closeQueue()
}

type FIFORequestQueue struct {
	queue chan Request
}

func NewFIFORequestQueue(queue chan Request) *FIFORequestQueue {
	return &FIFORequestQueue{queue: queue}
}

func (f *FIFORequestQueue) enqueueRequest(r Request) {
	f.queue <- r
}

func (f *FIFORequestQueue) dequeueRequest(_ int64, _ bool) Request {
	return <-f.queue
}

func (f *FIFORequestQueue) length() int {
	return len(f.queue)
}

func (f *FIFORequestQueue) closeQueue() {
	close(f.queue)
}

type PriorityRequestQueue struct {
	queue *util.PriorityQueue
}

func NewPriorityRequestQueue(queue *util.PriorityQueue) *PriorityRequestQueue {
	return &PriorityRequestQueue{queue: queue}
}

func (f *PriorityRequestQueue) enqueueRequest(r Request) {
	f.queue.Enqueue(r)
}

func (f *PriorityRequestQueue) dequeueRequest(minPriority int64, checkMinPriority bool) Request {
	if checkMinPriority && f.queue.Peek().Priority() < minPriority {
		return nil
	}
	return f.queue.Dequeue()
}

func (f *PriorityRequestQueue) length() int {
	return f.queue.Length()
}

func (f *PriorityRequestQueue) closeQueue() {
	f.queue.Close()
}
