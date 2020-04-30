package frontend

import (
	"container/list"
)

type queueRecord struct {
	ch     chan *request
	userID string
}

type queueManager struct {
	l          *list.List
	next       *list.Element
	userLookup map[string]*list.Element

	maxQueueSize int
}

func newQueueManager(maxQueueSize int) *queueManager {
	return &queueManager{
		l:            list.New(),
		next:         nil,
		userLookup:   make(map[string]*list.Element),
		maxQueueSize: maxQueueSize,
	}
}

func (q *queueManager) len() int {
	return len(q.userLookup)
}

func (q *queueManager) getNextQueue() (chan *request, string) {
	if q.next == nil {
		q.next = q.l.Front()
	}

	if q.next == nil {
		return nil, ""
	}

	var next *list.Element
	next, q.next = q.next, q.next.Next()

	qr := next.Value.(queueRecord)

	return qr.ch, qr.userID
}

func (q *queueManager) deleteQueue(userID string) {
	element := q.userLookup[userID]

	// remove from linked list
	if element != nil {
		if element == q.next {
			q.next = element.Next() // if we're deleting the current item just move to the next one
		}

		q.l.Remove(element)
	}

	// remove from map
	delete(q.userLookup, userID)
}

func (q *queueManager) getOrAddQueue(userID string) chan *request {
	element := q.userLookup[userID]

	if element == nil {
		qr := queueRecord{
			ch:     make(chan *request, q.maxQueueSize),
			userID: userID,
		}

		// add the element right before the current linked list item for fifo
		if q.next == nil {
			element = q.l.PushBack(qr)
		} else {
			element = q.l.InsertBefore(qr, q.next)
		}

		q.userLookup[userID] = element
	}

	return element.Value.(queueRecord).ch
}
