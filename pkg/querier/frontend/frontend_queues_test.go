package frontend

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFrontendQueues(t *testing.T) {
	m := newQueueManager()
	assert.NotNil(t, m)
	assert.NoError(t, m.isConsistent())
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
	assert.NoError(t, m.isConsistent())

	confirmOrder(t, m, qTwo, qThree, qTwo)

	qFour := getOrAddQueue(t, m, "four")

	confirmOrder(t, m, qThree, qTwo, qFour, qThree)

	// remove current and confirm round robin continues
	m.deleteQueue("two")
	assert.NoError(t, m.isConsistent())

	confirmOrder(t, m, qFour, qThree, qFour)

	m.deleteQueue("three")
	assert.NoError(t, m.isConsistent())

	m.deleteQueue("four")
	assert.NoError(t, m.isConsistent())

	q, _ = m.getNextQueue()
	assert.Nil(t, q)
}

func getOrAddQueue(t *testing.T, m *queueManager, tenant string) chan *request {
	q := m.getOrAddQueue(tenant)
	assert.NotNil(t, q)
	assert.NoError(t, m.isConsistent())
	assert.Equal(t, q, m.getOrAddQueue(tenant))

	return q
}

func confirmOrder(t *testing.T, m *queueManager, qs ...chan *request) {
	for _, q := range qs {
		qNext, _ := m.getNextQueue()
		assert.Equal(t, q, qNext)
		assert.NoError(t, m.isConsistent())
	}
}

func TestFrontendQueuesConsistency(t *testing.T) {
	m := newQueueManager()
	assert.NotNil(t, m)
	assert.NoError(t, m.isConsistent())
	q, _ := m.getNextQueue()
	assert.Nil(t, q)

	for i := 0; i < 1000; i++ {
		switch rand.Int() % 3 {
		case 0:
			assert.NotNil(t, m.getOrAddQueue(generateTenant()))
		case 1:
			m.getNextQueue()
		case 2:
			m.deleteQueue(generateTenant())
		}

		assert.NoErrorf(t, m.isConsistent(), "last action %d", i)
	}
}

func generateTenant() string {
	return fmt.Sprint("tenant-", rand.Int()%5)
}
