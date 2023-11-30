package queue

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"

	"github.com/cortexproject/cortex/pkg/util"
)

func TestFIFORequestQueue(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	queueLength := promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_query_scheduler_queue_length",
		Help: "Number of queries in the queue.",
	}, []string{"user", "priority", "type"})

	queue := NewFIFORequestQueue(make(chan Request, 2), "userID", queueLength)
	request1 := MockRequest{
		id:       "request 1",
		priority: 1,
	}
	request2 := MockRequest{
		id:       "request 2",
		priority: 2,
	}

	queue.enqueueRequest(request1)
	queue.enqueueRequest(request2)

	assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_scheduler_queue_length Number of queries in the queue.
		# TYPE cortex_query_scheduler_queue_length gauge
		cortex_query_scheduler_queue_length{priority="1",type="fifo",user="userID"} 1
		cortex_query_scheduler_queue_length{priority="2",type="fifo",user="userID"} 1
	`), "cortex_query_scheduler_queue_length"))
	assert.Equal(t, 2, queue.length())
	assert.Equal(t, request1, queue.dequeueRequest(0, false))
	assert.Equal(t, 1, queue.length())
	assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_scheduler_queue_length Number of queries in the queue.
		# TYPE cortex_query_scheduler_queue_length gauge
		cortex_query_scheduler_queue_length{priority="1",type="fifo",user="userID"} 0
		cortex_query_scheduler_queue_length{priority="2",type="fifo",user="userID"} 1
	`), "cortex_query_scheduler_queue_length"))
	assert.Equal(t, request2, queue.dequeueRequest(0, false))
	assert.Equal(t, 0, queue.length())
	assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_scheduler_queue_length Number of queries in the queue.
		# TYPE cortex_query_scheduler_queue_length gauge
		cortex_query_scheduler_queue_length{priority="1",type="fifo",user="userID"} 0
		cortex_query_scheduler_queue_length{priority="2",type="fifo",user="userID"} 0
	`), "cortex_query_scheduler_queue_length"))
}

func TestPriorityRequestQueue(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	queueLength := promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_query_scheduler_queue_length",
		Help: "Number of queries in the queue.",
	}, []string{"user", "priority", "type"})

	queue := NewPriorityRequestQueue(util.NewPriorityQueue(nil), "userID", queueLength)
	request1 := MockRequest{
		id:       "request 1",
		priority: 1,
	}
	request2 := MockRequest{
		id:       "request 2",
		priority: 2,
	}
	request3 := MockRequest{
		id:       "request 3",
		priority: 3,
	}

	queue.enqueueRequest(request1)
	queue.enqueueRequest(request2)
	assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_scheduler_queue_length Number of queries in the queue.
		# TYPE cortex_query_scheduler_queue_length gauge
		cortex_query_scheduler_queue_length{priority="1",type="priority",user="userID"} 1
		cortex_query_scheduler_queue_length{priority="2",type="priority",user="userID"} 1
	`), "cortex_query_scheduler_queue_length"))
	assert.Equal(t, 2, queue.length())
	assert.Equal(t, request2, queue.dequeueRequest(0, false))
	assert.Equal(t, 1, queue.length())
	assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_scheduler_queue_length Number of queries in the queue.
		# TYPE cortex_query_scheduler_queue_length gauge
		cortex_query_scheduler_queue_length{priority="1",type="priority",user="userID"} 1
		cortex_query_scheduler_queue_length{priority="2",type="priority",user="userID"} 0
	`), "cortex_query_scheduler_queue_length"))
	assert.Equal(t, request1, queue.dequeueRequest(0, false))
	assert.Equal(t, 0, queue.length())
	assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_scheduler_queue_length Number of queries in the queue.
		# TYPE cortex_query_scheduler_queue_length gauge
		cortex_query_scheduler_queue_length{priority="1",type="priority",user="userID"} 0
		cortex_query_scheduler_queue_length{priority="2",type="priority",user="userID"} 0
	`), "cortex_query_scheduler_queue_length"))

	queue.enqueueRequest(request1)
	queue.enqueueRequest(request2)
	queue.enqueueRequest(request3)
	assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_scheduler_queue_length Number of queries in the queue.
		# TYPE cortex_query_scheduler_queue_length gauge
		cortex_query_scheduler_queue_length{priority="1",type="priority",user="userID"} 1
		cortex_query_scheduler_queue_length{priority="2",type="priority",user="userID"} 1
		cortex_query_scheduler_queue_length{priority="3",type="priority",user="userID"} 1
	`), "cortex_query_scheduler_queue_length"))
	assert.Equal(t, 3, queue.length())
	assert.Equal(t, request3, queue.dequeueRequest(2, true))
	assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_scheduler_queue_length Number of queries in the queue.
		# TYPE cortex_query_scheduler_queue_length gauge
		cortex_query_scheduler_queue_length{priority="1",type="priority",user="userID"} 1
		cortex_query_scheduler_queue_length{priority="2",type="priority",user="userID"} 1
		cortex_query_scheduler_queue_length{priority="3",type="priority",user="userID"} 0
	`), "cortex_query_scheduler_queue_length"))
	assert.Equal(t, 2, queue.length())
	assert.Equal(t, request2, queue.dequeueRequest(2, true))
	assert.Equal(t, 1, queue.length())
	assert.Nil(t, queue.dequeueRequest(2, true))
	assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_scheduler_queue_length Number of queries in the queue.
		# TYPE cortex_query_scheduler_queue_length gauge
		cortex_query_scheduler_queue_length{priority="1",type="priority",user="userID"} 1
		cortex_query_scheduler_queue_length{priority="2",type="priority",user="userID"} 0
		cortex_query_scheduler_queue_length{priority="3",type="priority",user="userID"} 0
	`), "cortex_query_scheduler_queue_length"))
	assert.Equal(t, request1, queue.dequeueRequest(2, false))
	assert.Equal(t, 0, queue.length())
	assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_scheduler_queue_length Number of queries in the queue.
		# TYPE cortex_query_scheduler_queue_length gauge
		cortex_query_scheduler_queue_length{priority="1",type="priority",user="userID"} 0
		cortex_query_scheduler_queue_length{priority="2",type="priority",user="userID"} 0
		cortex_query_scheduler_queue_length{priority="3",type="priority",user="userID"} 0
	`), "cortex_query_scheduler_queue_length"))
}
