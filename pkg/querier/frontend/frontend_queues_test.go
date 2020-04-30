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
	assert.Nil(t, m.getNextQueue())

	// add queues
	qOne := getOrAddQueue(t, "one", m)
	qTwo := getOrAddQueue(t, "two", m)
	assert.NotEqual(t, qOne, qTwo)

	// confirm they come back in order and exhibit round robin
	qNext := m.getNextQueue()
	assert.Equal(t, qOne, qNext)
	assert.NoError(t, m.isConsistent())

	qNext = m.getNextQueue()
	assert.Equal(t, qTwo, qNext)
	assert.NoError(t, m.isConsistent())

	qNext = m.getNextQueue()
	assert.Equal(t, qOne, qNext)
	assert.NoError(t, m.isConsistent())

	// confirm fifo by adding a third queue and iterating to it
	qThree := getOrAddQueue(t, "three", m)
	assert.NotEqual(t, qOne, qThree)
	assert.NotEqual(t, qTwo, qThree)

	qNext = m.getNextQueue()
	assert.Equal(t, qTwo, qNext)
	assert.NoError(t, m.isConsistent())

	qNext = m.getNextQueue()
	assert.Equal(t, qOne, qNext)
	assert.NoError(t, m.isConsistent())

	qNext = m.getNextQueue()
	assert.Equal(t, qThree, qNext)
	assert.NoError(t, m.isConsistent())

	// remove one and round robin the others
	m.deleteQueue("one")
	assert.NoError(t, m.isConsistent())

	qNext = m.getNextQueue()
	assert.Equal(t, qTwo, qNext)
	assert.NoError(t, m.isConsistent())

	qNext = m.getNextQueue()
	assert.Equal(t, qThree, qNext)
	assert.NoError(t, m.isConsistent())

	qNext = m.getNextQueue()
	assert.Equal(t, qTwo, qNext)
	assert.NoError(t, m.isConsistent())

	// remove all
	m.deleteQueue("two")
	assert.NoError(t, m.isConsistent())

	m.deleteQueue("three")
	assert.NoError(t, m.isConsistent())

	assert.Nil(t, m.getNextQueue())
}

func getOrAddQueue(t *testing.T, tenant string, m *queueManager) requestQueue {
	q := m.getQueue(tenant)
	assert.NotNil(t, q)
	assert.NoError(t, m.isConsistent())
	assert.Equal(t, q, m.getQueue(tenant))

	return q
}

func TestFrontendQueuesConsistency(t *testing.T) {
	m := newQueueManager()
	assert.NotNil(t, m)
	assert.NoError(t, m.isConsistent())
	assert.Nil(t, m.getNextQueue())

	for i := 0; i < 1000; i++ {
		switch rand.Int() % 3 {
		case 0:
			assert.NotNil(t, m.getQueue(generateTenant()))
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
