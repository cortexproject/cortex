package frontend

import (
	"container/list"
	"fmt"
)

type requestQueue chan *request

type queueManager struct {
	l       *list.List
	queues  map[string]*list.Element
	current *list.Element
}

func newQueueManager() *queueManager {
	return &queueManager{
		l:       list.New(),
		queues:  make(map[string]*list.Element),
		current: nil,
	}
}

func (q *queueManager) getNextQueue() requestQueue {
	if q.current == nil {
		q.current = q.l.Front()
	}

	if q.current == nil {
		return nil
	}

	current := q.current
	q.current = q.current.Next() // advance to the next queue

	return current.Value.(requestQueue)
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

func (q *queueManager) getOrAddQueue(userID string) requestQueue {
	element := q.queues[userID]

	if element == nil {
		// need to add this userID.  add it right before the current linked list item for fifo
		if q.current == nil {
			element = q.l.PushBack(make(requestQueue))
		} else {
			element = q.l.InsertBefore(make(requestQueue), q.current)
		}

		q.queues[userID] = element
	}

	return element.Value.(requestQueue)
}

// isConsistent() returns true if every userID in the map is also in the linked list and vice versa.
//  This is horribly inefficient.  Use for testing only.
func (q *queueManager) isConsistent() error {
	// let's confirm that every element in the map is in the list and all values are request queues
	for k, v := range q.queues {
		found := false

		for e := q.l.Front(); e != nil; e = e.Next() {
			_, ok := e.Value.(requestQueue)
			if !ok {
				return fmt.Errorf("Element value is not requestQueue %v", e.Value)
			}

			if e == v {
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
