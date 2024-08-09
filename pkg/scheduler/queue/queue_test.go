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

	for n := 0; n < b.N; n++ {
		queue := NewRequestQueue(maxOutstandingPerTenant,
			prometheus.NewGaugeVec(prometheus.GaugeOpts{}, []string{"user", "priority", "type"}),
			prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"user", "priority"}),
			MockLimits{MaxOutstanding: 100},
			nil,
		)
		queues = append(queues, queue)

		for ix := 0; ix < queriers; ix++ {
			queue.RegisterQuerierConnection(fmt.Sprintf("querier-%d", ix))
		}

		for i := 0; i < maxOutstandingPerTenant; i++ {
			for j := 0; j < numTenants; j++ {
				userID := strconv.Itoa(j)

				err := queue.EnqueueRequest(userID, MockRequest{}, 0, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	}

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		idx := FirstUser()
		for j := 0; j < maxOutstandingPerTenant*numTenants; j++ {
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

	for n := 0; n < b.N; n++ {
		q := NewRequestQueue(maxOutstandingPerTenant,
			prometheus.NewGaugeVec(prometheus.GaugeOpts{}, []string{"user", "priority", "type"}),
			prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"user", "priority"}),
			MockLimits{MaxOutstanding: 100},
			nil,
		)

		for ix := 0; ix < queriers; ix++ {
			q.RegisterQuerierConnection(fmt.Sprintf("querier-%d", ix))
		}

		queues = append(queues, q)

		for j := 0; j < numTenants; j++ {
			requests = append(requests, MockRequest{id: fmt.Sprintf("%d-%d", n, j)})
			users = append(users, strconv.Itoa(j))
		}
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i := 0; i < maxOutstandingPerTenant; i++ {
			for j := 0; j < numTenants; j++ {
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

	for n := 0; n < b.N; n++ {
		queue := NewRequestQueue(maxOutstandingPerTenant,
			prometheus.NewGaugeVec(prometheus.GaugeOpts{}, []string{"user", "priority", "type"}),
			prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"user", "priority"}),
			MockLimits{MaxOutstanding: 100, QueryPriorityVal: validation.QueryPriority{Enabled: true}},
			nil,
		)
		queues = append(queues, queue)

		for ix := 0; ix < queriers; ix++ {
			queue.RegisterQuerierConnection(fmt.Sprintf("querier-%d", ix))
		}

		for i := 0; i < maxOutstandingPerTenant; i++ {
			for j := 0; j < numTenants; j++ {
				userID := strconv.Itoa(j)

				err := queue.EnqueueRequest(userID, MockRequest{priority: int64(i)}, 0, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	}

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		idx := FirstUser()
		for j := 0; j < maxOutstandingPerTenant*numTenants; j++ {
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

	for n := 0; n < b.N; n++ {
		q := NewRequestQueue(maxOutstandingPerTenant,
			prometheus.NewGaugeVec(prometheus.GaugeOpts{}, []string{"user", "priority", "type"}),
			prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"user", "priority"}),
			MockLimits{MaxOutstanding: 100, QueryPriorityVal: validation.QueryPriority{Enabled: true}},
			nil,
		)

		for ix := 0; ix < queriers; ix++ {
			q.RegisterQuerierConnection(fmt.Sprintf("querier-%d", ix))
		}

		queues = append(queues, q)

		for j := 0; j < numTenants; j++ {
			requests = append(requests, MockRequest{id: fmt.Sprintf("%d-%d", n, j), priority: int64(j)})
			users = append(users, strconv.Itoa(j))
		}
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i := 0; i < maxOutstandingPerTenant; i++ {
			for j := 0; j < numTenants; j++ {
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

	normalRequest := MockRequest{
		id: "normal query",
	}
	priority1Request := MockRequest{
		id:       "priority 1",
		priority: 1,
	}

	assert.NoError(t, queue.EnqueueRequest("userID", normalRequest, 1, func() {}))
	assert.NoError(t, queue.EnqueueRequest("userID", priority1Request, 1, func() {}))
	assert.NoError(t, queue.EnqueueRequest("userID", priority1Request, 1, func() {}))

	nextRequest, _, _ := queue.GetNextRequestForQuerier(ctx, FirstUser(), "querier-1")
	assert.Equal(t, priority1Request, nextRequest)

	nextRequest, _, _ = queue.GetNextRequestForQuerier(ctx, FirstUser(), "querier-1")
	assert.Equal(t, priority1Request, nextRequest)

	ctxTimeout, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	time.AfterFunc(2*time.Second, func() {
		queue.cond.Broadcast()
	})
	nextRequest, _, _ = queue.GetNextRequestForQuerier(ctxTimeout, FirstUser(), "querier-1")
	assert.Nil(t, nextRequest)
	assert.Equal(t, 1, queue.queues.userQueues["userID"].queue.length())

	assert.NoError(t, queue.EnqueueRequest("userID", normalRequest, 1, func() {}))

	ctxTimeout, cancel = context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	time.AfterFunc(2*time.Second, func() {
		queue.cond.Broadcast()
	})
	nextRequest, _, _ = queue.GetNextRequestForQuerier(ctxTimeout, FirstUser(), "querier-1")
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
