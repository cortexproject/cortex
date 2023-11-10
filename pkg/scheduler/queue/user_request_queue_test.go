package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cortexproject/cortex/pkg/util"
)

func TestFIFORequestQueue(t *testing.T) {
	queue := NewFIFORequestQueue(make(chan Request, 2))
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
	assert.Equal(t, 2, queue.length())
	assert.Equal(t, request1, queue.dequeueRequest(0, false))
	assert.Equal(t, 1, queue.length())
	assert.Equal(t, request2, queue.dequeueRequest(0, false))
	assert.Equal(t, 0, queue.length())
}

func TestPriorityRequestQueue(t *testing.T) {
	queue := NewPriorityRequestQueue(util.NewPriorityQueue(nil))
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
	assert.Equal(t, 2, queue.length())
	assert.Equal(t, request2, queue.dequeueRequest(0, true))
	assert.Equal(t, 1, queue.length())
	assert.Equal(t, request1, queue.dequeueRequest(0, true))
	assert.Equal(t, 0, queue.length())

	queue.enqueueRequest(request1)
	queue.enqueueRequest(request2)
	assert.Equal(t, 2, queue.length())
	assert.Equal(t, request2, queue.dequeueRequest(2, true))
	assert.Equal(t, 1, queue.length())
	assert.Nil(t, queue.dequeueRequest(2, true))
	assert.Equal(t, request1, queue.dequeueRequest(2, false))
	assert.Equal(t, 0, queue.length())
}
