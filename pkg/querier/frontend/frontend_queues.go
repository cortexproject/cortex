package frontend

import (
	"container/list"
	"fmt"
)

type queueRecord struct {
	q      chan *request
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

	return qr.q, qr.userID
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
			q:      make(chan *request, q.maxQueueSize),
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

	return element.Value.(queueRecord).q
}

// isConsistent() returns true if every userID in the map is also in the linked list and vice versa.
//  This is horribly inefficient.  Use for testing only.
func (q *queueManager) isConsistent() error {
	// let's confirm that every element in the map is in the list and all values are request queues
	for k, v := range q.queues {
		found := false

		for e := q.l.Front(); e != nil; e = e.Next() {
			qr, ok := e.Value.(queueRecord)
			if !ok {
				return fmt.Errorf("element value is not a queueRecord %v", e.Value)
			}

			if e == v {
				if qr.userID != k {
					return fmt.Errorf("mismatched userID between map %s and linked list %s", k, qr.userID)
				}

				found = true
				break
			}
		}

		if !found {
			return fmt.Errorf("userID %s not found in list", k)
		}
	}

	// now check the length to make sure there's not extra list items somehow
	listLen := q.l.Len()
	mapLen := len(q.queues)

	if listLen != mapLen {
		return fmt.Errorf("Length mismatch list:%d map:%d", listLen, mapLen)
	}

	return nil
}
