package frontend

import (
	"container/list"
)

type queueRecord struct {
	ch     chan *request
	userID string
}

type queueManager struct {
	l       *list.List
	queues  map[string]*list.Element
	current *list.Element

	maxQueueSize int
}

func newQueueManager(maxQueueSize int) *queueManager {
	return &queueManager{
		l:            list.New(),
		queues:       make(map[string]*list.Element),
		current:      nil,
		maxQueueSize: maxQueueSize,
	}
}

func (q *queueManager) len() int {
	return len(q.queues)
}

func (q *queueManager) getNextQueue() (chan *request, string) {
	if q.current == nil {
		q.current = q.l.Front()
	}

	if q.current == nil {
		return nil, ""
	}

	current := q.current
	q.current = q.current.Next() // advance to the next queue

	qr := current.Value.(queueRecord)

	return qr.ch, qr.userID
}

func (q *queueManager) deleteQueue(userID string) {
	element := q.queues[userID]

	// remove from linked list
	if element != nil {
		if element == q.current {
			q.current = element.Next()
		}

		q.l.Remove(element)
	}

	// remove from map
	delete(q.queues, userID)
}

func (q *queueManager) getOrAddQueue(userID string) chan *request {
	element := q.queues[userID]

	if element == nil {
		qr := queueRecord{
			ch:     make(chan *request, q.maxQueueSize),
			userID: userID,
		}

		// need to add this userID.  add it right before the current linked list item for fifo
		if q.current == nil {
			element = q.l.PushBack(qr)
		} else {
			element = q.l.InsertBefore(qr, q.current)
		}

		q.queues[userID] = element
	}

	return element.Value.(queueRecord).ch
}
