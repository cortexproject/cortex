package frontend

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFrontendQueues(t *testing.T) {
	m := newQueueIterator(0)
	assert.NotNil(t, m)
	assert.NoError(t, isConsistent(m))
	q, _ := m.getNextQueue()
	assert.Nil(t, q)

	// add queues
	qOne := getOrAddQueue(t, m, "one")

	confirmOrder(t, m, qOne, qOne)

	qTwo := getOrAddQueue(t, m, "two")
	assert.NotEqual(t, qOne, qTwo)

	confirmOrder(t, m, qOne, qTwo, qOne)

	// confirm fifo by adding a third queue and iterating to it
	qThree := getOrAddQueue(t, m, "three")

	confirmOrder(t, m, qTwo, qOne, qThree)

	// remove one and round robin the others
	m.deleteQueue("one")
	assert.NoError(t, isConsistent(m))

	confirmOrder(t, m, qTwo, qThree, qTwo)

	qFour := getOrAddQueue(t, m, "four")

	confirmOrder(t, m, qThree, qTwo, qFour, qThree)

	// remove current and confirm round robin continues
	m.deleteQueue("two")
	assert.NoError(t, isConsistent(m))

	confirmOrder(t, m, qFour, qThree, qFour)

	m.deleteQueue("three")
	assert.NoError(t, isConsistent(m))

	m.deleteQueue("four")
	assert.NoError(t, isConsistent(m))

	q, _ = m.getNextQueue()
	assert.Nil(t, q)
}

func TestFrontendQueuesConsistency(t *testing.T) {
	m := newQueueIterator(0)
	assert.NotNil(t, m)
	assert.NoError(t, isConsistent(m))
	q, _ := m.getNextQueue()
	assert.Nil(t, q)

	r := rand.New(rand.NewSource(time.Now().Unix()))

	for i := 0; i < 1000; i++ {
		switch r.Int() % 3 {
		case 0:
			assert.NotNil(t, m.getOrAddQueue(generateTenant(r)))
		case 1:
			m.getNextQueue()
		case 2:
			m.deleteQueue(generateTenant(r))
		}

		assert.NoErrorf(t, isConsistent(m), "last action %d", i)
	}
}

func generateTenant(r *rand.Rand) string {
	return fmt.Sprint("tenant-", r.Int()%5)
}

func getOrAddQueue(t *testing.T, m *queueIterator, tenant string) chan *request {
	q := m.getOrAddQueue(tenant)
	assert.NotNil(t, q)
	assert.NoError(t, isConsistent(m))
	assert.Equal(t, q, m.getOrAddQueue(tenant))

	return q
}

func confirmOrder(t *testing.T, m *queueIterator, qs ...chan *request) {
	for _, q := range qs {
		qNext, _ := m.getNextQueue()
		assert.Equal(t, q, qNext)
		assert.NoError(t, isConsistent(m))
	}
}

// isConsistent() returns true if every userID in the map is also in the linked list and vice versa.
//  This is horribly inefficient.  Use for testing only.
func isConsistent(q *queueIterator) error {
	// let's confirm that every element in the map is in the list and all values are request queues
	for k, v := range q.userLookup {
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
	mapLen := len(q.userLookup)

	if listLen != mapLen {
		return fmt.Errorf("Length mismatch list:%d map:%d", listLen, mapLen)
	}

	return nil
}
