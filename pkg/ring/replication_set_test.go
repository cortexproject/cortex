package ring

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/querier/partialdata"
	"github.com/cortexproject/cortex/pkg/util/validation"
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

func TestReplicationSet_GetAddressesWithout(t *testing.T) {
	tests := map[string]struct {
		rs       ReplicationSet
		expected []string
		exclude  string
	}{
		"should return an empty slice on empty replication set": {
			rs:       ReplicationSet{},
			expected: []string{},
			exclude:  "127.0.0.1",
		},
		"non-matching exclusion, should return all addresses": {
			rs: ReplicationSet{
				Instances: []InstanceDesc{
					{Addr: "127.0.0.1"},
					{Addr: "127.0.0.2"},
					{Addr: "127.0.0.3"},
				},
			},
			expected: []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"},
			exclude:  "127.0.0.4",
		},
		"matching exclusion, should return non-excluded addresses": {
			rs: ReplicationSet{
				Instances: []InstanceDesc{
					{Addr: "127.0.0.1"},
					{Addr: "127.0.0.2"},
					{Addr: "127.0.0.3"},
				},
			},
			expected: []string{"127.0.0.1", "127.0.0.3"},
			exclude:  "127.0.0.2",
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.ElementsMatch(t, testData.expected, testData.rs.GetAddressesWithout(testData.exclude))
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
		zoneResultsQuorum   bool
		queryPartialData    bool
		errStrContains      []string
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
			name:      "with partial data enabled and max unavailable zones = 1, should succeed on instances failing in 2 out of 3 zones (6 instances)",
			instances: []InstanceDesc{{Addr: "10.0.0.1", Zone: "zone1"}, {Addr: "10.0.0.2", Zone: "zone2"}, {Addr: "10.0.0.3", Zone: "zone3"}, {Addr: "10.0.0.4", Zone: "zone1"}, {Addr: "10.0.0.5", Zone: "zone2"}, {Addr: "10.0.0.6", Zone: "zone3"}},
			f: func(ctx context.Context, ing *InstanceDesc) (interface{}, error) {
				if ing.Addr == "10.0.0.1" || ing.Addr == "10.0.0.2" {
					return nil, errZoneFailure
				}
				return 1, nil
			},
			maxUnavailableZones: 1,
			queryPartialData:    true,
			want:                []interface{}{1, 1, 1, 1},
			expectedError:       partialdata.ErrPartialData,
			errStrContains:      []string{"10.0.0.1", "10.0.0.2", "zone failed"},
		},
		{
			name:                "with partial data enabled, should fail on instances failing in all zones",
			instances:           []InstanceDesc{{Zone: "zone1"}, {Zone: "zone2"}, {Zone: "zone3"}, {Zone: "zone2"}, {Zone: "zone3"}},
			f:                   failingFunctionOnZones("zone1", "zone2", "zone3"),
			maxUnavailableZones: 1,
			expectedError:       errZoneFailure,
			queryPartialData:    true,
		},
		{
			name:      "with partial data enabled, should fail on instances returning 422",
			instances: []InstanceDesc{{Addr: "1", Zone: "zone1"}, {Addr: "2", Zone: "zone2"}, {Addr: "3", Zone: "zone3"}, {Addr: "4", Zone: "zone1"}, {Addr: "5", Zone: "zone2"}, {Addr: "6", Zone: "zone3"}},
			f: func(ctx context.Context, ing *InstanceDesc) (interface{}, error) {
				if ing.Addr == "1" || ing.Addr == "2" {
					return nil, validation.LimitError("limit breached")
				}
				return 1, nil
			},
			maxUnavailableZones: 1,
			expectedError:       validation.LimitError("limit breached"),
			queryPartialData:    true,
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
		{
			name:      "max unavailable zones = 1, zoneResultsQuorum = true, should contain 4 results (2 from zone1, 2 from zone2)",
			instances: []InstanceDesc{{Zone: "zone1"}, {Zone: "zone2"}, {Zone: "zone3"}, {Zone: "zone1"}, {Zone: "zone2"}, {Zone: "zone3"}},
			f: func(c context.Context, id *InstanceDesc) (interface{}, error) {
				return 1, nil
			},
			maxUnavailableZones: 1,
			want:                []interface{}{1, 1, 1, 1},
			zoneResultsQuorum:   true,
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
			got, err := r.Do(ctx, tt.delay, tt.zoneResultsQuorum, tt.queryPartialData, tt.f)
			if tt.expectedError != nil {
				assert.ErrorContains(t, err, tt.expectedError.Error())
				for _, str := range tt.errStrContains {
					assert.ErrorContains(t, err, str)
				}
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

var (
	replicationSetChangesInitialState = ReplicationSet{
		Instances: []InstanceDesc{
			{Addr: "127.0.0.1"},
			{Addr: "127.0.0.2"},
			{Addr: "127.0.0.3"},
		},
	}
	replicationSetChangesTestCases = map[string]struct {
		nextState                                  ReplicationSet
		expectHasReplicationSetChanged             bool
		expectHasReplicationSetChangedWithoutState bool
	}{
		"timestamp changed": {
			ReplicationSet{
				Instances: []InstanceDesc{
					{Addr: "127.0.0.1", Timestamp: time.Hour.Microseconds()},
					{Addr: "127.0.0.2"},
					{Addr: "127.0.0.3"},
				},
			},
			false,
			false,
		},
		"state changed": {
			ReplicationSet{
				Instances: []InstanceDesc{
					{Addr: "127.0.0.1", State: PENDING},
					{Addr: "127.0.0.2"},
					{Addr: "127.0.0.3"},
				},
			},
			true,
			false,
		},
		"more instances": {
			ReplicationSet{
				Instances: []InstanceDesc{
					{Addr: "127.0.0.1"},
					{Addr: "127.0.0.2"},
					{Addr: "127.0.0.3"},
					{Addr: "127.0.0.4"},
				},
			},
			true,
			true,
		},
		"less instances": {
			ReplicationSet{
				Instances: []InstanceDesc{
					{Addr: "127.0.0.1"},
					{Addr: "127.0.0.2"},
				},
			},
			true,
			true,
		},
		"replaced instance": {
			ReplicationSet{
				Instances: []InstanceDesc{
					{Addr: "127.0.0.1"},
					{Addr: "127.0.0.2"},
					{Addr: "127.0.0.5"},
				},
			},
			true,
			true,
		},
	}
)

func TestHasReplicationSetChanged_IgnoresTimeStamp(t *testing.T) {
	// Only testing difference to underlying Equal function
	for testName, testData := range replicationSetChangesTestCases {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expectHasReplicationSetChanged, HasReplicationSetChanged(replicationSetChangesInitialState, testData.nextState), "HasReplicationSetChanged wrong result")
		})
	}
}

func TestHasReplicationSetChangedWithoutState_IgnoresTimeStampAndState(t *testing.T) {
	// Only testing difference to underlying Equal function
	for testName, testData := range replicationSetChangesTestCases {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expectHasReplicationSetChangedWithoutState, HasReplicationSetChangedWithoutState(replicationSetChangesInitialState, testData.nextState), "HasReplicationSetChangedWithoutState wrong result")
		})
	}
}
