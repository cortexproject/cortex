package ring

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestReplicationSet_GetAddresses(t *testing.T) {
	tests := map[string]struct {
		rs       ReplicationSet
		expected []string
	}{
		"should return an empty slice on empty replication set": {
			rs:       ReplicationSet{},
			expected: []string{},
		},
		"should return instances addresses (no order guaranteed)": {
			rs: ReplicationSet{
				Instances: []InstanceDesc{
					{Addr: "127.0.0.1"},
					{Addr: "127.0.0.2"},
					{Addr: "127.0.0.3"},
				},
			},
			expected: []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.ElementsMatch(t, testData.expected, testData.rs.GetAddresses())
		})
	}
}

var (
	errFailure     = errors.New("failed")
	errZoneFailure = errors.New("zone failed")
)

// Return a function that fails starting from failAfter times
func failingFunctionAfter(failAfter int32, delay time.Duration) func(context.Context, *InstanceDesc) (interface{}, error) {
	count := atomic.NewInt32(0)
	return func(context.Context, *InstanceDesc) (interface{}, error) {
		time.Sleep(delay)
		if count.Inc() > failAfter {
			return nil, errFailure
		}
		return 1, nil
	}
}

func failingFunctionOnZones(zones ...string) func(context.Context, *InstanceDesc) (interface{}, error) {
	return func(ctx context.Context, ing *InstanceDesc) (interface{}, error) {
		for _, zone := range zones {
			if ing.Zone == zone {
				return nil, errZoneFailure
			}
		}
		return 1, nil
	}
}

func TestReplicationSet_Do(t *testing.T) {
	tests := []struct {
		name                string
		instances           []InstanceDesc
		maxErrors           int
		maxUnavailableZones int
		f                   func(context.Context, *InstanceDesc) (interface{}, error)
		delay               time.Duration
		cancelContextDelay  time.Duration
		want                []interface{}
		expectedError       error
	}{
		{
			name: "max errors = 0, no errors no delay",
			instances: []InstanceDesc{
				{},
			},
			f: func(c context.Context, id *InstanceDesc) (interface{}, error) {
				return 1, nil
			},
			want: []interface{}{1},
		},
		{
			name:      "max errors = 0, should fail on 1 error out of 1 instance",
			instances: []InstanceDesc{{}},
			f: func(c context.Context, id *InstanceDesc) (interface{}, error) {
				return nil, errFailure
			},
			want:          nil,
			expectedError: errFailure,
		},
		{
			name:          "max errors = 0, should fail on 1 error out of 3 instances (last call fails)",
			instances:     []InstanceDesc{{}, {}, {}},
			f:             failingFunctionAfter(2, 10*time.Millisecond),
			want:          nil,
			expectedError: errFailure,
		},
		{
			name:          "max errors = 1, should fail on 3 errors out of 5 instances (last calls fail)",
			instances:     []InstanceDesc{{}, {}, {}, {}, {}},
			maxErrors:     1,
			f:             failingFunctionAfter(2, 10*time.Millisecond),
			delay:         100 * time.Millisecond,
			want:          nil,
			expectedError: errFailure,
		},
		{
			name:      "max errors = 1, should handle context canceled",
			instances: []InstanceDesc{{}, {}, {}},
			maxErrors: 1,
			f: func(c context.Context, id *InstanceDesc) (interface{}, error) {
				time.Sleep(300 * time.Millisecond)
				return 1, nil
			},
			cancelContextDelay: 100 * time.Millisecond,
			want:               nil,
			expectedError:      context.Canceled,
		},
		{
			name:      "max errors = 0, should succeed on all successful instances",
			instances: []InstanceDesc{{Zone: "zone1"}, {Zone: "zone2"}, {Zone: "zone3"}},
			f: func(c context.Context, id *InstanceDesc) (interface{}, error) {
				return 1, nil
			},
			want: []interface{}{1, 1, 1},
		},
		{
			name:                "max unavailable zones = 1, should succeed on instances failing in 1 out of 3 zones (3 instances)",
			instances:           []InstanceDesc{{Zone: "zone1"}, {Zone: "zone2"}, {Zone: "zone3"}},
			f:                   failingFunctionOnZones("zone1"),
			maxUnavailableZones: 1,
			want:                []interface{}{1, 1},
		},
		{
			name:                "max unavailable zones = 1, should fail on instances failing in 2 out of 3 zones (3 instances)",
			instances:           []InstanceDesc{{Zone: "zone1"}, {Zone: "zone2"}, {Zone: "zone3"}},
			f:                   failingFunctionOnZones("zone1", "zone2"),
			maxUnavailableZones: 1,
			expectedError:       errZoneFailure,
		},
		{
			name:                "max unavailable zones = 1, should succeed on instances failing in 1 out of 3 zones (6 instances)",
			instances:           []InstanceDesc{{Zone: "zone1"}, {Zone: "zone1"}, {Zone: "zone2"}, {Zone: "zone2"}, {Zone: "zone3"}, {Zone: "zone3"}},
			f:                   failingFunctionOnZones("zone1"),
			maxUnavailableZones: 1,
			want:                []interface{}{1, 1, 1, 1},
		},
		{
			name:                "max unavailable zones = 2, should fail on instances failing in 3 out of 5 zones (5 instances)",
			instances:           []InstanceDesc{{Zone: "zone1"}, {Zone: "zone2"}, {Zone: "zone3"}, {Zone: "zone4"}, {Zone: "zone5"}},
			f:                   failingFunctionOnZones("zone1", "zone2", "zone3"),
			maxUnavailableZones: 2,
			expectedError:       errZoneFailure,
		},
		{
			name:                "max unavailable zones = 2, should succeed on instances failing in 2 out of 5 zones (10 instances)",
			instances:           []InstanceDesc{{Zone: "zone1"}, {Zone: "zone1"}, {Zone: "zone2"}, {Zone: "zone2"}, {Zone: "zone3"}, {Zone: "zone3"}, {Zone: "zone4"}, {Zone: "zone4"}, {Zone: "zone5"}, {Zone: "zone5"}},
			f:                   failingFunctionOnZones("zone1", "zone5"),
			maxUnavailableZones: 2,
			want:                []interface{}{1, 1, 1, 1, 1, 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Ensure the test case has been correctly setup (max errors and max unavailable zones are
			// mutually exclusive).
			require.False(t, tt.maxErrors > 0 && tt.maxUnavailableZones > 0)

			r := ReplicationSet{
				Instances:           tt.instances,
				MaxErrors:           tt.maxErrors,
				MaxUnavailableZones: tt.maxUnavailableZones,
			}
			ctx := context.Background()
			if tt.cancelContextDelay > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				time.AfterFunc(tt.cancelContextDelay, func() {
					cancel()
				})
			}
			got, err := r.Do(ctx, tt.delay, tt.f)
			if tt.expectedError != nil {
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}
