package frontend

import "container/list"

type requestQueue chan *request

type queueManager struct {
	l       *list.List
	queues  map[string]requestQueue
	current *list.Element
}

func newQueueManager() *queueManager {
	return &queueManager{
		l:       list.New(),
		queues:  make(map[string]requestQueue),
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

	tenant := q.current.Value.(string)

	return q.queues[tenant]
}

func (q *queueManager) deleteQueue(tenant string) {

}

func (q *queueManager) getQueue(tenant string) requestQueue {

}
