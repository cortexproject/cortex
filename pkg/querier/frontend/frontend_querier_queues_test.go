package frontend

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQueues(t *testing.T) {
	uq := newUserQueues(0)
	assert.NotNil(t, uq)
	assert.NoError(t, isConsistent(uq))

	q, u, lastUid := uq.getNextQueueForQuerier(-1, "querier-1")
	assert.Nil(t, q)
	assert.Equal(t, "", u)

	// Add queues: [one]
	qOne := getOrAdd(t, uq, "one", 0)
	lastUid = confirmOrderForQuerier(t, uq, "querier-1", lastUid, qOne, qOne)

	// [one two]
	qTwo := getOrAdd(t, uq, "two", 0)
	assert.NotEqual(t, qOne, qTwo)

	lastUid = confirmOrderForQuerier(t, uq, "querier-1", lastUid, qTwo, qOne, qTwo, qOne)
	confirmOrderForQuerier(t, uq, "querier-2", -1, qOne, qTwo, qOne)

	// [one two three]
	// confirm fifo by adding a third queue and iterating to it
	qThree := getOrAdd(t, uq, "three", 0)

	lastUid = confirmOrderForQuerier(t, uq, "querier-1", lastUid, qTwo, qThree, qOne)

	// Remove one: ["" two three]
	uq.deleteQueue("one")
	assert.NoError(t, isConsistent(uq))

	lastUid = confirmOrderForQuerier(t, uq, "querier-1", lastUid, qTwo, qThree, qTwo)

	// "four" is added at the beginning of the list: [four two three]
	qFour := getOrAdd(t, uq, "four", 0)

	lastUid = confirmOrderForQuerier(t, uq, "querier-1", lastUid, qThree, qFour, qTwo, qThree)

	// Remove two: [four "" three]
	uq.deleteQueue("two")
	assert.NoError(t, isConsistent(uq))

	lastUid = confirmOrderForQuerier(t, uq, "querier-1", lastUid, qFour, qThree, qFour)

	// Remove three: [four]
	uq.deleteQueue("three")
	assert.NoError(t, isConsistent(uq))

	// Remove four: []
	uq.deleteQueue("four")
	assert.NoError(t, isConsistent(uq))

	q, _, _ = uq.getNextQueueForQuerier(lastUid, "querier-1")
	assert.Nil(t, q)
}

func TestQueuesWithQueriers(t *testing.T) {
	uq := newUserQueues(0)
	assert.NotNil(t, uq)
	assert.NoError(t, isConsistent(uq))

	queriers := 10
	users := 100
	maxQueriersPerUser := 3

	// Add some queriers.
	for ix := 0; ix < queriers; ix++ {
		qid := fmt.Sprintf("querier-%d", ix)
		uq.addQuerierConnection(qid)

		// No querier has any queues yet.
		q, u, _ := uq.getNextQueueForQuerier(-1, qid)
		assert.Nil(t, q)
		assert.Equal(t, "", u)
	}

	assert.NoError(t, isConsistent(uq))

	// Add user queues.
	for u := 0; u < users; u++ {
		uid := fmt.Sprintf("user-%d", u)
		getOrAdd(t, uq, uid, 3)

		// Verify it has 3 queriers assigned now.
		qs := uq.userQueues[uid].queriers
		assert.Equal(t, maxQueriersPerUser, len(qs))
	}

	// After adding all users, verify results. For each querier, find out how many different users it handles.
	serversMap := make(map[string]int)

	for q := 0; q < queriers; q++ {
		qid := fmt.Sprintf("querier-%d", q)

		lastUid := -1
		for {
			_, _, newUid := uq.getNextQueueForQuerier(lastUid, qid)
			if newUid < lastUid {
				break
			}
			lastUid = newUid
			serversMap[qid] += 1
		}
	}

	mean := float64(0)
	for _, c := range serversMap {
		mean += float64(c)
	}
	mean = mean / float64(len(serversMap))

	stdDev := float64(0)
	for _, c := range serversMap {
		d := float64(c) - mean
		stdDev += (d * d)
	}
	stdDev = math.Sqrt(stdDev / float64(len(serversMap)))

	assert.InDelta(t, users*maxQueriersPerUser/queriers, mean, 1)
	assert.InDelta(t, stdDev, 0, 5)
}

func TestQueuesConsistency(t *testing.T) {
	uq := newUserQueues(0)
	assert.NotNil(t, uq)
	assert.NoError(t, isConsistent(uq))

	r := rand.New(rand.NewSource(time.Now().Unix()))

	lastUids := map[string]int{}

	for i := 0; i < 1000; i++ {
		switch r.Int() % 6 {
		case 0:
			assert.NotNil(t, uq.getOrAddQueue(generateTenant(r), 3))
		case 1:
			qid := generateQuerier(r)
			_, _, luid := uq.getNextQueueForQuerier(lastUids[qid], qid)
			lastUids[qid] = luid
		case 2:
			uq.deleteQueue(generateTenant(r))
		case 3:
			uq.addQuerierConnection(generateQuerier(r))
		case 4:
			uq.removeQuerierConnection(generateQuerier(r))
		}

		assert.NoErrorf(t, isConsistent(uq), "last action %d", i)
	}
}

func generateTenant(r *rand.Rand) string {
	return fmt.Sprint("tenant-", r.Int()%5)
}

func generateQuerier(r *rand.Rand) string {
	return fmt.Sprint("querier-", r.Int()%5)
}

func getOrAdd(t *testing.T, uq *queues, tenant string, maxQueriers int) chan *request {
	q := uq.getOrAddQueue(tenant, maxQueriers)
	assert.NotNil(t, q)
	assert.NoError(t, isConsistent(uq))
	assert.Equal(t, q, uq.getOrAddQueue(tenant, maxQueriers))
	return q
}

func confirmOrderForQuerier(t *testing.T, uq *queues, querier string, lastUid int, qs ...chan *request) int {
	var n chan *request
	for _, q := range qs {
		n, _, lastUid = uq.getNextQueueForQuerier(lastUid, querier)
		assert.Equal(t, q, n)
		assert.NoError(t, isConsistent(uq))
	}
	return lastUid
}

func isConsistent(uq *queues) error {
	if len(uq.sortedQueriers) != len(uq.querierConnections) {
		return fmt.Errorf("inconsistent number of sorted queriers and querier connections")
	}

	uc := 0
	for ix, u := range uq.users {
		q := uq.userQueues[u]
		if u != "" && q == nil {
			return fmt.Errorf("user %s doesn't have queue", u)
		}
		if u == "" && q != nil {
			return fmt.Errorf("user %s doesn't have queue", u)
		}
		if u == "" {
			continue
		}

		uc++

		if q.index != ix {
			return fmt.Errorf("invalid user's index, expected=%d, got=%d", ix, q.index)
		}

		if q.maxQueriers == 0 && q.queriers != nil {
			return fmt.Errorf("user %s has queriers, but maxQueriers=0", u)
		}

		if q.maxQueriers > 0 && len(uq.sortedQueriers) <= q.maxQueriers && q.queriers != nil {
			return fmt.Errorf("user %s has queriers set despite not enough queriers available", u)
		}

		if q.maxQueriers > 0 && len(uq.sortedQueriers) > q.maxQueriers && len(q.queriers) != q.maxQueriers {
			return fmt.Errorf("user %s has incorrect number of queriers, expected=%d, got=%d", u, len(q.queriers), q.maxQueriers)
		}
	}

	if uc != len(uq.userQueues) {
		return fmt.Errorf("inconsistent number of users list and user queues")
	}

	return nil
}
