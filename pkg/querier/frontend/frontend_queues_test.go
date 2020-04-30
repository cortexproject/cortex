package frontend

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	actionGetQueue = iota
	actionGetNextQueue
	actionDeleteQueue
)

func TestFrontendQueues(t *testing.T) {
	m := newQueueManager()
	assert.NotNil(t, m)
	assert.NoError(t, m.isConsistent())

	q := m.getQueue("blerg")
	assert.NotNil(t, q)
	assert.NoError(t, m.isConsistent())

	qNext := m.getNextQueue()
	assert.Equal(t, q, qNext)
	assert.NoError(t, m.isConsistent())

	m.deleteQueue("blerg")
	assert.NoError(t, m.isConsistent())
}

func TestFrontendQueuesConsistency(t *testing.T) {
	type queueAction struct {
		action int
		tenant string
	}

	tests := []struct {
		name    string
		actions []queueAction
	}{
		{
			name:    "Test No Action",
			actions: []queueAction{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newQueueManager()
			assert.NotNil(t, m)
			assert.NoError(t, m.isConsistent())

			for _, a := range tt.actions {
				switch a.action {
				case actionGetQueue:
					assert.NotNil(t, m.getQueue(a.tenant))
				case actionGetNextQueue:
					assert.NotNil(t, m.getNextQueue())
				case actionDeleteQueue:
					m.deleteQueue(a.tenant)
				}
			}

			assert.NoError(t, m.isConsistent())
		})
	}
}
