package ring

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIngesterDesc_IsHealthy(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		ingester      *IngesterDesc
		timeout       time.Duration
		writeExpected bool
		readExpected  bool
	}{
		"ALIVE ingester with last keepalive newer than timeout": {
			ingester:      &IngesterDesc{State: ACTIVE, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:       time.Minute,
			writeExpected: true,
			readExpected:  true,
		},
		"ALIVE ingester with last keepalive older than timeout": {
			ingester:      &IngesterDesc{State: ACTIVE, Timestamp: time.Now().Add(-90 * time.Second).Unix()},
			timeout:       time.Minute,
			writeExpected: false,
			readExpected:  false,
		},
		"JOINING ingester with last keepalive newer than timeout": {
			ingester:      &IngesterDesc{State: JOINING, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:       time.Minute,
			writeExpected: false,
			readExpected:  false,
		},
		"LEAVING ingester with last keepalive newer than timeout": {
			ingester:      &IngesterDesc{State: LEAVING, Timestamp: time.Now().Add(-30 * time.Second).Unix()},
			timeout:       time.Minute,
			writeExpected: false,
			readExpected:  true,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			actual := testData.ingester.IsHealthy(Write, testData.timeout)
			assert.Equal(t, testData.writeExpected, actual)

			actual = testData.ingester.IsHealthy(Read, testData.timeout)
			assert.Equal(t, testData.readExpected, actual)
		})
	}
}
