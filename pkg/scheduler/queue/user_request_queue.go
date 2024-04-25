package queue

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/util"
)

type userRequestQueue interface {
	enqueueRequest(Request)
	dequeueRequest(int64, bool) Request
	length() int
}

type FIFORequestQueue struct {
	queue       chan Request
	userID      string
	queueLength *prometheus.GaugeVec
}

func NewFIFORequestQueue(queue chan Request, userID string, queueLength *prometheus.GaugeVec) *FIFORequestQueue {
	return &FIFORequestQueue{queue: queue, userID: userID, queueLength: queueLength}
}

func (f *FIFORequestQueue) enqueueRequest(r Request) {
	f.queue <- r
	if f.queueLength != nil {
		f.queueLength.WithLabelValues(f.userID, strconv.FormatInt(r.Priority(), 10), "fifo").Inc()
	}
}

func (f *FIFORequestQueue) dequeueRequest(_ int64, _ bool) Request {
	r := <-f.queue
	if f.queueLength != nil {
		f.queueLength.WithLabelValues(f.userID, strconv.FormatInt(r.Priority(), 10), "fifo").Dec()
	}
	return r
}

func (f *FIFORequestQueue) length() int {
	return len(f.queue)
}

type PriorityRequestQueue struct {
	queue       *util.PriorityQueue
	userID      string
	queueLength *prometheus.GaugeVec
}

func NewPriorityRequestQueue(queue *util.PriorityQueue, userID string, queueLength *prometheus.GaugeVec) *PriorityRequestQueue {
	return &PriorityRequestQueue{queue: queue, userID: userID, queueLength: queueLength}
}

func (f *PriorityRequestQueue) enqueueRequest(r Request) {
	f.queue.Enqueue(r)
	if f.queueLength != nil {
		f.queueLength.WithLabelValues(f.userID, strconv.FormatInt(r.Priority(), 10), "priority").Inc()
	}
}

func (f *PriorityRequestQueue) dequeueRequest(minPriority int64, checkMinPriority bool) Request {
	if checkMinPriority && f.queue.Peek().Priority() < minPriority {
		return nil
	}
	r := f.queue.Dequeue()
	if f.queueLength != nil {
		f.queueLength.WithLabelValues(f.userID, strconv.FormatInt(r.Priority(), 10), "priority").Dec()
	}
	return r
}

func (f *PriorityRequestQueue) length() int {
	return f.queue.Length()
}
