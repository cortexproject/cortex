package queue

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func BenchmarkGetNextRequest(b *testing.B) {
	const maxOutstandingPerTenant = 2
	const numTenants = 50
	const queriers = 5

	queues := make([]*RequestQueue, 0, b.N)

	for b.Loop() {
		queue := NewRequestQueue(maxOutstandingPerTenant,
			prometheus.NewGaugeVec(prometheus.GaugeOpts{}, []string{"user", "priority", "type"}),
			prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"user", "priority"}),
			MockLimits{MaxOutstanding: 100},
			nil,
		)
		queues = append(queues, queue)

		for ix := range queriers {
			queue.RegisterQuerierConnection(fmt.Sprintf("querier-%d", ix))
		}

		for range maxOutstandingPerTenant {
			for j := range numTenants {
				userID := strconv.Itoa(j)

				err := queue.EnqueueRequest(userID, MockRequest{}, 0, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	}

	ctx := context.Background()

	for i := 0; b.Loop(); i++ {
		idx := FirstUser()
		for range maxOutstandingPerTenant * numTenants {
			querier := ""
		b:
			// Find querier with at least one request to avoid blocking in getNextRequestForQuerier.
			for _, q := range queues[i].queues.userQueues {
				for qid := range q.queriers {
					querier = qid
					break b
				}
			}

			_, nidx, err := queues[i].GetNextRequestForQuerier(ctx, idx, querier)
			if err != nil {
				b.Fatal(err)
			}
			idx = nidx
		}
	}
}

func BenchmarkQueueRequest(b *testing.B) {
	const maxOutstandingPerTenant = 2
	const numTenants = 50
	const queriers = 5

	queues := make([]*RequestQueue, 0, b.N)
	users := make([]string, 0, numTenants)
	requests := make([]MockRequest, 0, numTenants)

	for n := 0; b.Loop(); n++ {
		q := NewRequestQueue(maxOutstandingPerTenant,
			prometheus.NewGaugeVec(prometheus.GaugeOpts{}, []string{"user", "priority", "type"}),
			prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"user", "priority"}),
			MockLimits{MaxOutstanding: 100},
			nil,
		)

		for ix := range queriers {
			q.RegisterQuerierConnection(fmt.Sprintf("querier-%d", ix))
		}

		queues = append(queues, q)

		for j := range numTenants {
			requests = append(requests, MockRequest{id: fmt.Sprintf("%d-%d", n, j)})
			users = append(users, strconv.Itoa(j))
		}
	}

	for n := 0; b.Loop(); n++ {
		for range maxOutstandingPerTenant {
			for j := range numTenants {
				err := queues[n].EnqueueRequest(users[j], requests[j], 0, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	}
}

func BenchmarkGetNextRequestPriorityQueue(b *testing.B) {
	const maxOutstandingPerTenant = 2
	const numTenants = 50
	const queriers = 5

	queues := make([]*RequestQueue, 0, b.N)

	for b.Loop() {
		queue := NewRequestQueue(maxOutstandingPerTenant,
			prometheus.NewGaugeVec(prometheus.GaugeOpts{}, []string{"user", "priority", "type"}),
			prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"user", "priority"}),
			MockLimits{MaxOutstanding: 100, QueryPriorityVal: validation.QueryPriority{Enabled: true}},
			nil,
		)
		queues = append(queues, queue)

		for ix := range queriers {
			queue.RegisterQuerierConnection(fmt.Sprintf("querier-%d", ix))
		}

		for i := range maxOutstandingPerTenant {
			for j := range numTenants {
				userID := strconv.Itoa(j)

				err := queue.EnqueueRequest(userID, MockRequest{priority: int64(i)}, 0, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	}

	ctx := context.Background()

	for i := 0; b.Loop(); i++ {
		idx := FirstUser()
		for range maxOutstandingPerTenant * numTenants {
			querier := ""
		b:
			// Find querier with at least one request to avoid blocking in getNextRequestForQuerier.
			for _, q := range queues[i].queues.userQueues {
				for qid := range q.queriers {
					querier = qid
					break b
				}
			}

			_, nidx, err := queues[i].GetNextRequestForQuerier(ctx, idx, querier)
			if err != nil {
				b.Fatal(err)
			}
			idx = nidx
		}
	}
}

func BenchmarkQueueRequestPriorityQueue(b *testing.B) {
	const maxOutstandingPerTenant = 2
	const numTenants = 50
	const queriers = 5

	queues := make([]*RequestQueue, 0, b.N)
	users := make([]string, 0, numTenants)
	requests := make([]MockRequest, 0, numTenants)

	for n := 0; b.Loop(); n++ {
		q := NewRequestQueue(maxOutstandingPerTenant,
			prometheus.NewGaugeVec(prometheus.GaugeOpts{}, []string{"user", "priority", "type"}),
			prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"user", "priority"}),
			MockLimits{MaxOutstanding: 100, QueryPriorityVal: validation.QueryPriority{Enabled: true}},
			nil,
		)

		for ix := range queriers {
			q.RegisterQuerierConnection(fmt.Sprintf("querier-%d", ix))
		}

		queues = append(queues, q)

		for j := range numTenants {
			requests = append(requests, MockRequest{id: fmt.Sprintf("%d-%d", n, j), priority: int64(j)})
			users = append(users, strconv.Itoa(j))
		}
	}

	for n := 0; b.Loop(); n++ {
		for range maxOutstandingPerTenant {
			for j := range numTenants {
				err := queues[n].EnqueueRequest(users[j], requests[j], 0, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	}
}

func TestRequestQueue_GetNextRequestForQuerier_ShouldGetRequestAfterReshardingBecauseQuerierHasBeenForgotten(t *testing.T) {
	const forgetDelay = 3 * time.Second

	queue := NewRequestQueue(forgetDelay,
		prometheus.NewGaugeVec(prometheus.GaugeOpts{}, []string{"user", "priority", "type"}),
		prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"user", "priority"}),
		MockLimits{MaxOutstanding: 100},
		nil,
	)

	// Start the queue service.
	ctx := context.Background()
	require.NoError(t, services.StartAndAwaitRunning(ctx, queue))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, queue))
	})

	// Two queriers connect.
	queue.RegisterQuerierConnection("querier-1")
	queue.RegisterQuerierConnection("querier-2")

	// Querier-2 waits for a new request.
	querier2wg := sync.WaitGroup{}
	querier2wg.Add(1)
	go func() {
		defer querier2wg.Done()
		_, _, err := queue.GetNextRequestForQuerier(ctx, FirstUser(), "querier-2")
		require.NoError(t, err)
	}()

	// Querier-1 crashes (no graceful shutdown notification).
	queue.UnregisterQuerierConnection("querier-1")

	// Enqueue a request from an user which would be assigned to querier-1.
	// NOTE: "user-1" hash falls in the querier-1 shard.
	require.NoError(t, queue.EnqueueRequest("user-1", MockRequest{}, 1, nil))

	startTime := time.Now()
	querier2wg.Wait()
	waitTime := time.Since(startTime)

	// We expect that querier-2 got the request only after querier-1 forget delay is passed.
	assert.GreaterOrEqual(t, waitTime.Milliseconds(), forgetDelay.Milliseconds())
}

func TestQueriersShouldGetHighPriorityQueryFirst(t *testing.T) {
	queue := NewRequestQueue(0,
		prometheus.NewGaugeVec(prometheus.GaugeOpts{}, []string{"user", "priority", "type"}),
		prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"user", "priority"}),
		MockLimits{MaxOutstanding: 3, QueryPriorityVal: validation.QueryPriority{Enabled: true}},
		nil,
	)
	ctx := context.Background()
	queue.RegisterQuerierConnection("querier-1")

	normalRequest1 := MockRequest{
		id: "normal query 1",
	}
	normalRequest2 := MockRequest{
		id: "normal query 2",
	}
	highPriorityRequest := MockRequest{
		id:       "high priority query",
		priority: 1,
	}

	assert.NoError(t, queue.EnqueueRequest("userID", normalRequest1, 1, func() {}))
	assert.NoError(t, queue.EnqueueRequest("userID", normalRequest2, 1, func() {}))
	assert.NoError(t, queue.EnqueueRequest("userID", highPriorityRequest, 1, func() {}))

	assert.Error(t, queue.EnqueueRequest("userID", highPriorityRequest, 1, func() {})) // should fail due to maxOutstandingPerTenant = 3
	nextRequest, _, _ := queue.GetNextRequestForQuerier(ctx, FirstUser(), "querier-1")
	assert.Equal(t, highPriorityRequest, nextRequest) // high priority request returned, although it was enqueued the last
}

func TestGetOrAddQueue_ShouldNotDeadlockWhenLimitsAreReduced(t *testing.T) {
	// Setup: Large initial limit
	initialLimit := 100
	newLimit := 50

	limits := MockLimits{
		MaxOutstanding: initialLimit,
	}

	// Initialize queues
	q := newUserQueues(0, limits, nil)

	// Create user queue
	userID := "test-user-deadlock"
	queue := q.getOrAddQueue(userID, 1)

	// Fill queue to capacity (near initialLimit)
	// We fill it more than newLimit
	itemsToFill := 80
	for range itemsToFill {
		queue.enqueueRequest(MockRequest{priority: 1})
	}

	require.Equal(t, itemsToFill, queue.length())

	// Reduce limit below current size
	// We change the mock limits return value.
	// In real app this comes from runtime config reload.
	limits.MaxOutstanding = newLimit
	q.limits = limits // Update strict reference in queues struct (mocking the reload effect)

	// Now call getOrAddQueue again.
	// This triggers the migration logic: existing queue (80 items) -> new queue (cap 50).
	done := make(chan struct{})
	go func() {
		_ = q.getOrAddQueue(userID, 1)
		close(done)
	}()

	select {
	case <-done:
		// Success: no deadlock
	case <-time.After(2 * time.Second):
		t.Fatal("Deadlock detected! getOrAddQueue timed out while migrating queue with reduced limits.")
	}

	// The new queue should be capped at newLimit or contain what managed to fit.
	// Logic: it breaks when full. So new queue should be full (length == newLimit).
	newQueue := q.getOrAddQueue(userID, 1) // Should be fast now

	// Note: The actual items in queue should be newLimit (50). The rest (30) are dropped.
	assert.Equal(t, newLimit, newQueue.length())
}

func TestReservedQueriersShouldOnlyGetHighPriorityQueries(t *testing.T) {
	queue := NewRequestQueue(0,
		prometheus.NewGaugeVec(prometheus.GaugeOpts{}, []string{"user", "priority", "type"}),
		prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"user", "priority"}),
		MockLimits{
			MaxOutstanding: 3,
			QueryPriorityVal: validation.QueryPriority{
				Enabled: true,
				Priorities: []validation.PriorityDef{
					{
						Priority:         1,
						ReservedQueriers: 1,
					},
				},
			},
		},
		nil,
	)
	ctx := context.Background()

	queue.RegisterQuerierConnection("querier-1")
	queue.RegisterQuerierConnection("querier-2")
	maxQueriers := float64(2)

	normalRequest := MockRequest{
		id: "normal query",
	}
	priority1Request := MockRequest{
		id:       "priority 1",
		priority: 1,
	}

	assert.NoError(t, queue.EnqueueRequest("userID", normalRequest, maxQueriers, func() {}))
	assert.NoError(t, queue.EnqueueRequest("userID", priority1Request, maxQueriers, func() {}))
	assert.NoError(t, queue.EnqueueRequest("userID", priority1Request, maxQueriers, func() {}))
	reservedQueriers := queue.queues.userQueues["userID"].reservedQueriers
	require.Equal(t, 1, len(reservedQueriers))
	reservedQuerier := ""
	for qid := range reservedQueriers {
		reservedQuerier = qid
	}

	nextRequest, _, _ := queue.GetNextRequestForQuerier(ctx, FirstUser(), reservedQuerier)
	assert.Equal(t, priority1Request, nextRequest)

	nextRequest, _, _ = queue.GetNextRequestForQuerier(ctx, FirstUser(), reservedQuerier)
	assert.Equal(t, priority1Request, nextRequest)

	ctxTimeout, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	time.AfterFunc(2*time.Second, func() {
		queue.cond.Broadcast()
	})
	nextRequest, _, _ = queue.GetNextRequestForQuerier(ctxTimeout, FirstUser(), reservedQuerier)
	assert.Nil(t, nextRequest)
	assert.Equal(t, 1, queue.queues.userQueues["userID"].queue.length())

	assert.NoError(t, queue.EnqueueRequest("userID", normalRequest, maxQueriers, func() {}))

	ctxTimeout, cancel = context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	time.AfterFunc(2*time.Second, func() {
		queue.cond.Broadcast()
	})
	nextRequest, _, _ = queue.GetNextRequestForQuerier(ctxTimeout, FirstUser(), reservedQuerier)
	assert.Nil(t, nextRequest)
	assert.Equal(t, 2, queue.queues.userQueues["userID"].queue.length())
}

func TestExitingRequestsShouldPersistEvenIfTheConfigHasChanged(t *testing.T) {
	limits := MockLimits{
		MaxOutstanding: 3,
	}
	queue := NewRequestQueue(0,
		prometheus.NewGaugeVec(prometheus.GaugeOpts{}, []string{"user", "priority", "type"}),
		prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"user", "priority"}),
		limits,
		nil,
	)

	ctx := context.Background()

	queue.RegisterQuerierConnection("querier-1")

	normalRequest := MockRequest{
		id: "normal query",
	}
	highPriorityRequest := MockRequest{
		id:       "high priority query",
		priority: 1,
	}

	assert.NoError(t, queue.EnqueueRequest("userID", normalRequest, 1, func() {}))
	assert.NoError(t, queue.EnqueueRequest("userID", highPriorityRequest, 1, func() {}))
	assert.NoError(t, queue.EnqueueRequest("userID", normalRequest, 1, func() {}))

	limits.MaxOutstanding = 4
	limits.QueryPriorityVal = validation.QueryPriority{Enabled: true}
	queue.queues.limits = limits

	assert.NoError(t, queue.EnqueueRequest("userID", normalRequest, 1, func() {}))

	nextRequest, _, _ := queue.GetNextRequestForQuerier(ctx, FirstUser(), "querier-1")
	assert.Equal(t, highPriorityRequest, nextRequest)

	nextRequest, _, _ = queue.GetNextRequestForQuerier(ctx, FirstUser(), "querier-1")
	assert.Equal(t, normalRequest, nextRequest)
	assert.Equal(t, 2, queue.queues.userQueues["userID"].queue.length())
}

type MockRequest struct {
	id       string
	priority int64
}

func (r MockRequest) Priority() int64 {
	return r.priority
}
