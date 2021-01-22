package alertmanager

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cortexproject/cortex/pkg/ring"
)

func TestIsHealthyForAlertmanagerOperations(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		instance *ring.IngesterDesc
		timeout  time.Duration
		expected bool
	}{
		"ACTIVE instance with last keepalive newer than timeout": {
			instance: &ring.IngesterDesc{State: ring.ACTIVE, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:  time.Minute,
			expected: true,
		},
		"ACTIVE instance with last keepalive older than timeout": {
			instance: &ring.IngesterDesc{State: ring.ACTIVE, Timestamp: time.Now().Add(-90 * time.Second).Unix()},
			timeout:  time.Minute,
			expected: false,
		},
		"JOINING instance with last keepalive newer than timeout": {
			instance: &ring.IngesterDesc{State: ring.JOINING, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:  time.Minute,
			expected: false,
		},
		"LEAVING instance with last keepalive newer than timeout": {
			instance: &ring.IngesterDesc{State: ring.LEAVING, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:  time.Minute,
			expected: false,
		},
		"PENDING instance with last keepalive newer than timeout": {
			instance: &ring.IngesterDesc{State: ring.PENDING, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:  time.Minute,
			expected: false,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			actual := testData.instance.IsHealthy(RingOp, testData.timeout, time.Now())
			assert.Equal(t, testData.expected, actual)
		})
	}
}
