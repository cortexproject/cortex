package ring

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
				Ingesters: []IngesterDesc{
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
func failingFunctionAfter(failAfter int, delay time.Duration) func(context.Context, *IngesterDesc) (interface{}, error) {
	var mutex = &sync.RWMutex{}
func failingFunctionAfter(failAfter int32, delay time.Duration) func(context.Context, *IngesterDesc) (interface{}, error) {
	count := atomic.NewInt32(0)
	return func(context.Context, *IngesterDesc) (interface{}, error) {
		time.Sleep(delay)
		if count.Inc() > failAfter {
			return nil, errFailure
		}
		return 1, nil
	}
}
	return func(context.Context, *IngesterDesc) (interface{}, error) {
		mutex.Lock()
		count++
		mutex.Unlock()
		time.Sleep(delay)
		mutex.RLock()
		defer mutex.RUnlock()
		if count > failAfter {
			return nil, errFailure
		}
		return 1, nil
	}
}

func failingFunctionForZones(zones ...string) func(context.Context, *IngesterDesc) (interface{}, error) {
	return func(ctx context.Context, ing *IngesterDesc) (interface{}, error) {
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
		instances           []IngesterDesc
		maxErrors           int
		maxUnavailableZones int
		f                   func(context.Context, *IngesterDesc) (interface{}, error)
		delay               time.Duration
		cancelContextDelay  time.Duration
		want                []interface{}
		expectedError       error
	}{
		{
			name: "no errors no delay",
			instances: []IngesterDesc{
				{},
			},
			f: func(c context.Context, id *IngesterDesc) (interface{}, error) {
				return 1, nil
			},
			want: []interface{}{1},
		},
		{
			name:      "1 error, no errors expected",
			instances: []IngesterDesc{{}},
			f: func(c context.Context, id *IngesterDesc) (interface{}, error) {
				return nil, errFailure
			},
			want:          nil,
			expectedError: errFailure,
		},
		{
			name:          "3 instances, last call fails",
			instances:     []IngesterDesc{{}, {}, {}},
			f:             failingFunctionAfter(2, 10*time.Millisecond),
			want:          nil,
			expectedError: errFailure,
		},
		{
			name:          "5 instances, with delay, last 3 calls fail",
			instances:     []IngesterDesc{{}, {}, {}, {}, {}},
			maxErrors:     1,
			f:             failingFunctionAfter(2, 10*time.Millisecond),
			delay:         100 * time.Millisecond,
			want:          nil,
			expectedError: errFailure,
		},
		{
			name:      "3 instances, context cancelled",
			instances: []IngesterDesc{{}, {}, {}},
			maxErrors: 1,
			f: func(c context.Context, id *IngesterDesc) (interface{}, error) {
				time.Sleep(300 * time.Millisecond)
				return 1, nil
			},
			cancelContextDelay: 100 * time.Millisecond,
			want:               nil,
			expectedError:      context.Canceled,
		},
		{
			name: "3 instances, 3 zones, no failures",
			instances: []IngesterDesc{{
				Zone: "zone1",
			}, {
				Zone: "zone2",
			}, {
				Zone: "zone3",
			}},
			f: func(c context.Context, id *IngesterDesc) (interface{}, error) {
				return 1, nil
			},
			want: []interface{}{1, 1, 1},
		},
		{
			name: "3 instances, 3 zones, 1 zone fails",
			instances: []IngesterDesc{{
				Zone: "zone1",
			}, {
				Zone: "zone2",
			}, {
				Zone: "zone3",
			}},
			f:                   failingFunctionForZones("zone1"),
			maxUnavailableZones: 1, // (nr of zones) / 2
			maxErrors:           1, // (nr of instances / nr of zones) * maxUnavailableZones
			want:                []interface{}{1, 1},
		},
		{
			name: "3 instances, 3 zones, 2 zones fail",
			instances: []IngesterDesc{{
				Zone: "zone1",
			}, {
				Zone: "zone2",
			}, {
				Zone: "zone3",
			}},
			f:                   failingFunctionForZones("zone1", "zone2"),
			maxUnavailableZones: 1,
			maxErrors:           1,
			expectedError:       errorTooManyZoneFailures,
		},
		{
			name: "6 instances, 3 zones, 1 zone fails",
			instances: []IngesterDesc{{
				Zone: "zone1",
			}, {
				Zone: "zone1",
			}, {
				Zone: "zone2",
			}, {
				Zone: "zone2",
			}, {
				Zone: "zone3",
			}, {
				Zone: "zone3",
			}},
			f:                   failingFunctionForZones("zone1"),
			maxUnavailableZones: 1,
			maxErrors:           2,
			want:                []interface{}{1, 1, 1, 1},
		},
		{
			name: "5 instances, 5 zones, 3 zones fails",
			instances: []IngesterDesc{{
				Zone: "zone1",
			}, {
				Zone: "zone2",
			}, {
				Zone: "zone3",
			}, {
				Zone: "zone4",
			}, {
				Zone: "zone5",
			}},
			f:                   failingFunctionForZones("zone1", "zone2", "zone3"),
			maxUnavailableZones: 2,
			maxErrors:           2,
			expectedError:       errorTooManyZoneFailures,
		},
		{
			name: "10 instances, 5 zones, 2 failures in zone 1",
			instances: []IngesterDesc{{
				Zone: "zone1",
			}, {
				Zone: "zone1",
			}, {
				Zone: "zone2",
			}, {
				Zone: "zone2",
			}, {
				Zone: "zone3",
			}, {
				Zone: "zone3",
			}, {
				Zone: "zone4",
			}, {
				Zone: "zone4",
			}, {
				Zone: "zone5",
			}, {
				Zone: "zone5",
			}},
			f:                   failingFunctionForZones("zone1"),
			maxUnavailableZones: 2,
			maxErrors:           4,
			want:                []interface{}{1, 1, 1, 1, 1, 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := ReplicationSet{
				Ingesters:           tt.instances,
				MaxErrors:           tt.maxErrors,
				MaxUnavailableZones: tt.maxUnavailableZones,
			}
			ctx := context.Background()
			if tt.cancelContextDelay > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				go func() {
					time.AfterFunc(tt.cancelContextDelay, func() {
						cancel()
					})
				}()
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
