package servicediscovery

import (
	"testing"
	"time"

	"github.com/bmizerany/assert"
)

func TestServiceRegistryDesc_ensureInstance(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		instanceID        string
		now               time.Time
		instances         map[string]ServiceInstanceDesc
		expectedCreated   bool
		expectedInstances map[string]ServiceInstanceDesc
	}{
		"should create the first instance if the registry is empty": {
			instanceID:      "i-1",
			now:             time.Unix(1, 0),
			instances:       nil,
			expectedCreated: true,
			expectedInstances: map[string]ServiceInstanceDesc{
				"i-1": {Id: "i-1", Timestamp: time.Unix(1, 0).UnixNano()},
			},
		},
		"should update the timestamp of the instance if already registered": {
			instanceID: "i-1",
			now:        time.Unix(10, 0),
			instances: map[string]ServiceInstanceDesc{
				"i-1": {Id: "i-1", Timestamp: time.Unix(1, 0).UnixNano()},
			},
			expectedCreated: false,
			expectedInstances: map[string]ServiceInstanceDesc{
				"i-1": {Id: "i-1", Timestamp: time.Unix(10, 0).UnixNano()},
			},
		},
		"should register a new instance if not already registered and the register is not empty": {
			instanceID: "i-2",
			now:        time.Unix(10, 0),
			instances: map[string]ServiceInstanceDesc{
				"i-1": {Id: "i-1", Timestamp: time.Unix(1, 0).UnixNano()},
			},
			expectedCreated: true,
			expectedInstances: map[string]ServiceInstanceDesc{
				"i-1": {Id: "i-1", Timestamp: time.Unix(1, 0).UnixNano()},
				"i-2": {Id: "i-2", Timestamp: time.Unix(10, 0).UnixNano()},
			},
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			registry := NewServiceRegistryDesc()
			registry.Instances = testData.instances
			created := registry.ensureInstance(testData.instanceID, testData.now)

			assert.Equal(t, testData.expectedCreated, created)
			assert.Equal(t, testData.expectedInstances, registry.Instances)
		})
	}
}

func TestServiceRegistryDesc_removeExpired(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		now               time.Time
		timeout           time.Duration
		instances         map[string]ServiceInstanceDesc
		expectedInstances map[string]ServiceInstanceDesc
	}{
		"should not remove any instance if none has expired": {
			now:     time.Unix(10, 0),
			timeout: 20 * time.Second,
			instances: map[string]ServiceInstanceDesc{
				"i-1": {Id: "i-1", Timestamp: time.Unix(1, 0).UnixNano()},
				"i-2": {Id: "i-2", Timestamp: time.Unix(5, 0).UnixNano()},
			},
			expectedInstances: map[string]ServiceInstanceDesc{
				"i-1": {Id: "i-1", Timestamp: time.Unix(1, 0).UnixNano()},
				"i-2": {Id: "i-2", Timestamp: time.Unix(5, 0).UnixNano()},
			},
		},
		"should expired instances and keep alive ones": {
			now:     time.Unix(30, 0),
			timeout: 20 * time.Second,
			instances: map[string]ServiceInstanceDesc{
				"i-1": {Id: "i-1", Timestamp: time.Unix(9, 0).UnixNano()},
				"i-2": {Id: "i-2", Timestamp: time.Unix(11, 0).UnixNano()},
			},
			expectedInstances: map[string]ServiceInstanceDesc{
				"i-2": {Id: "i-2", Timestamp: time.Unix(11, 0).UnixNano()},
			},
		},
		"should remove all instances if all have expired": {
			now:     time.Unix(40, 0),
			timeout: 20 * time.Second,
			instances: map[string]ServiceInstanceDesc{
				"i-1": {Id: "i-1", Timestamp: time.Unix(9, 0).UnixNano()},
				"i-2": {Id: "i-2", Timestamp: time.Unix(11, 0).UnixNano()},
			},
			expectedInstances: map[string]ServiceInstanceDesc{},
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			registry := NewServiceRegistryDesc()
			registry.Instances = testData.instances

			registry.removeExpired(testData.timeout, testData.now)
			assert.Equal(t, testData.expectedInstances, registry.Instances)
		})
	}
}
