package queue

import "github.com/cortexproject/cortex/pkg/util"

type requestQueue interface {
	enqueueRequest(Request)
	dequeueRequest() Request
	length() int
}

type FIFORequestQueue struct {
	queue chan Request
}

func NewFIFOQueue(queue chan Request) *FIFORequestQueue {
	return &FIFORequestQueue{queue: queue}
}

func (f *FIFORequestQueue) enqueueRequest(r Request) {
	f.queue <- r
}

func (f *FIFORequestQueue) dequeueRequest() Request {
	return <-f.queue
}

func (f *FIFORequestQueue) length() int {
	return len(f.queue)
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

func (f *PriorityRequestQueue) dequeueRequest() Request {
	return f.queue.Dequeue()
}

func (f *PriorityRequestQueue) length() int {
	return f.queue.Length()
}
