package storegateway

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cortexproject/cortex/pkg/ring"
)

func TestIsHealthyForStoreGatewayOperations(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		instance      *ring.InstanceDesc
		timeout       time.Duration
		syncExpected  bool
		queryExpected bool
	}{
		"ACTIVE instance with last keepalive newer than timeout": {
			instance:      &ring.InstanceDesc{State: ring.ACTIVE, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:       time.Minute,
			syncExpected:  true,
			queryExpected: true,
		},
		"ACTIVE instance with last keepalive older than timeout": {
			instance:      &ring.InstanceDesc{State: ring.ACTIVE, Timestamp: time.Now().Add(-90 * time.Second).Unix()},
			timeout:       time.Minute,
			syncExpected:  false,
			queryExpected: false,
		},
		"JOINING instance with last keepalive newer than timeout": {
			instance:      &ring.InstanceDesc{State: ring.JOINING, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:       time.Minute,
			syncExpected:  true,
			queryExpected: false,
		},
		"LEAVING instance with last keepalive newer than timeout": {
			instance:      &ring.InstanceDesc{State: ring.LEAVING, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:       time.Minute,
			syncExpected:  true,
			queryExpected: false,
		},
		"PENDING instance with last keepalive newer than timeout": {
			instance:      &ring.InstanceDesc{State: ring.PENDING, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:       time.Minute,
			syncExpected:  false,
			queryExpected: false,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			actual := testData.instance.IsHealthy(BlocksSync, testData.timeout, time.Now())
			assert.Equal(t, testData.syncExpected, actual)

			actual = testData.instance.IsHealthy(BlocksRead, testData.timeout, time.Now())
			assert.Equal(t, testData.queryExpected, actual)
		})
	}
}
